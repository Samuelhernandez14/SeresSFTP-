import json
import os
import queue
import shutil
import stat
import threading
import time
from itertools import islice

import paramiko
from dotenv import load_dotenv

load_dotenv()

SFTP_HOST = (os.environ["SFTP_HOST"].strip()
             .replace("sftp://", "").replace("ssh://", "").strip("/"))
SFTP_PORT = int(os.environ.get("SFTP_PORT", 22))
SFTP_USER = os.environ["SFTP_USER"]
SFTP_PASS = os.environ["SFTP_PASS"]
RUTA_REMOTA = os.environ.get("RUTA_REMOTA", ".")
RUTA_DESTINO = os.environ["RUTA_DESTINO"]
RUTA_LOG = os.environ.get("RUTA_LOG", "./descarga_sftp.log")
OMITIR = os.environ.get("OMITIR_EXISTENTES", "true").lower() == "true"
REINTENTOS = int(os.environ.get("REINTENTOS", 5))
LIMITE = int(os.environ.get("LIMITE_ARCHIVOS", 0) or 0)   # 0 = sin limite
HILOS = max(1, int(os.environ.get("HILOS", 3)))           # conexiones en paralelo
PAUSA_CONEXION = float(os.environ.get("PAUSA_CONEXION", 1.5))  # segundos entre conexion y conexion
CACHE = os.environ.get("CACHE_LISTADO", "./listado_cache.json")
USAR_CACHE = os.environ.get("USAR_CACHE", "true").lower() == "true"
ORDEN = os.environ.get("ORDEN", "antiguos").lower()      # antiguos | grandes
DIAS_RETENCION = int(os.environ.get("DIAS_RETENCION", 15))  # el servidor borra pasado este tiempo

INTERVALO_PROGRESO = 10   # segundos entre mensajes de progreso del listado

os.makedirs(RUTA_DESTINO, exist_ok=True)

lock = threading.Lock()
log = open(RUTA_LOG, "a", encoding="utf-8")
stop = threading.Event()
dirs_ok = set()
cont = {"copiado": 0, "omitido": 0, "fallido": 0, "hecho": 0, "bytes": 0, "desaparecido": 0}


def escribir(msg):
    with lock:
        print(msg, flush=True)
        log.write(msg + "\n")
        log.flush()


def fmt(n):
    if n >= 1024 ** 3:
        return f"{n / 1024 ** 3:.2f} GB"
    if n >= 1024 ** 2:
        return f"{n / 1024 ** 2:.1f} MB"
    return f"{n / 1024:.1f} KB"


def conectar():
    """Abre una conexion SFTP nueva. Devuelve [transport, sftp]."""
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.set_keepalive(30)
    # Servidor antiguo (ProFTPD mod_sftp): solo ofrece llave de host "ssh-rsa"
    opts = transport.get_security_options()
    try:
        if "ssh-rsa" not in opts.key_types:
            opts.key_types = tuple(opts.key_types) + ("ssh-rsa",)
    except ValueError:
        escribir('Esta version de paramiko no soporta ssh-rsa. '
                 'Instala: python -m pip install "paramiko==3.5.1"')
        raise
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    return [transport, paramiko.SFTPClient.from_transport(transport)]


def conectar_reintentos(etiqueta, intentos=4):
    """Conecta con reintentos y espera creciente (el servidor puede rechazar rafagas)."""
    for i in range(1, intentos + 1):
        try:
            return conectar()
        except Exception as e:
            escribir(f"[{etiqueta}] conexion fallo ({i}/{intentos}): {e}")
            if i == intentos:
                raise
            time.sleep(3 * i)


def cerrar(conn):
    for obj in reversed(conn or []):
        try:
            obj.close()
        except Exception:
            pass


def listar(sftp, ruta):
    """Recorre recursivamente y devuelve (ruta_remota, tamano)."""
    for item in sftp.listdir_attr(ruta):
        r = f"{ruta.rstrip('/')}/{item.filename}"
        if stat.S_ISDIR(item.st_mode):
            yield from listar(sftp, r)
        elif stat.S_ISREG(item.st_mode):
            yield r, item.st_size, item.st_mtime


def listar_paralelo(raiz):
    """Lista carpetas en paralelo con varias conexiones. Devuelve (archivos, errores)."""
    pendientes = queue.Queue()
    pendientes.put(raiz)
    archivos, errores = [], []
    est = {"pend": 1, "dirs": 0}   # carpetas en cola + en proceso
    listo = threading.Event()

    def lworker(num):
        try:
            conn = conectar_reintentos(f"listado {num}")
        except Exception as e:
            escribir(f"[listado {num}] no pudo conectar, se omite este hilo: {e}")
            return
        try:
            while not listo.is_set() and not stop.is_set():
                try:
                    d = pendientes.get(timeout=0.5)
                except queue.Empty:
                    continue
                subdirs, files = [], []
                try:
                    items = None
                    for intento in range(3):
                        try:
                            items = conn[1].listdir_attr(d)
                            break
                        except Exception as e:
                            escribir(f"  error listando {d} (intento {intento + 1}/3): {e}")
                            time.sleep(2)
                            try:
                                cerrar(conn)
                                conn[:] = conectar()
                            except Exception:
                                pass
                    if items is None:
                        with lock:
                            errores.append(d)
                    else:
                        for it in items:
                            r = f"{d.rstrip('/')}/{it.filename}"
                            if stat.S_ISDIR(it.st_mode):
                                subdirs.append(r)
                            elif stat.S_ISREG(it.st_mode):
                                files.append((r, it.st_size, it.st_mtime))
                finally:
                    with lock:
                        archivos.extend(files)
                        est["dirs"] += 1
                        est["pend"] += len(subdirs)   # contar hijos antes de cerrar el padre
                    for sd in subdirs:
                        pendientes.put(sd)
                    with lock:
                        est["pend"] -= 1
                        if est["pend"] == 0:
                            listo.set()
        finally:
            cerrar(conn)

    n = HILOS
    hilos = [threading.Thread(target=lworker, args=(i + 1,), daemon=True) for i in range(n)]
    for h in hilos:
        h.start()
        time.sleep(PAUSA_CONEXION)
    escribir(f"Listando con {n} conexiones en paralelo ...")
    ult = time.time()
    try:
        while not listo.is_set() and any(h.is_alive() for h in hilos):
            time.sleep(0.5)
            if time.time() - ult >= INTERVALO_PROGRESO:
                with lock:
                    msg = (f"  listado: {est['dirs']} carpetas revisadas, "
                           f"{len(archivos)} archivos encontrados, "
                           f"{est['pend']} carpetas por revisar")
                escribir(msg)   # fuera del lock: escribir() ya lo usa (si no, se bloquea)
                ult = time.time()
    except KeyboardInterrupt:
        stop.set()
        escribir("Listado interrumpido.")
        raise
    for h in hilos:
        h.join(timeout=5)
    if not listo.is_set():
        errores.append("(listado incompleto: los hilos se quedaron sin conexion)")
    escribir(f"Listado completo: {est['dirs']} carpetas, {len(archivos)} archivos")
    return archivos, errores


def ruta_local(remoto):
    base = RUTA_REMOTA
    rel = os.path.relpath(remoto, base) if base not in (".", "/") else remoto.lstrip("/")
    return os.path.join(RUTA_DESTINO, rel)


def descargar(conn, remoto, tam, mtime):
    destino = ruta_local(remoto)
    carpeta = os.path.dirname(destino)
    with lock:
        nueva = carpeta not in dirs_ok
        if nueva:
            dirs_ok.add(carpeta)
    if nueva:
        os.makedirs(carpeta, exist_ok=True)

    if OMITIR:
        try:
            if os.stat(destino).st_size == tam:
                return "omitido"
        except FileNotFoundError:
            pass

    parcial = destino + ".part"
    for intento in range(1, REINTENTOS + 1):
        try:
            # open + prefetch(tam): evita pedir el tamano de nuevo (1 viaje menos por archivo)
            with conn[1].open(remoto, "rb") as f:
                if tam > 0:
                    f.prefetch(tam)
                with open(parcial, "wb") as out:
                    shutil.copyfileobj(f, out, 1024 * 1024)
            if os.path.getsize(parcial) != tam:
                raise IOError("tamano distinto al original")
            os.replace(parcial, destino)
            try:
                os.utime(destino, (mtime, mtime))   # conserva la fecha original del servidor
            except Exception:
                pass
            return "copiado"
        except FileNotFoundError:
            # el servidor ya lo borro (retencion): no tiene sentido reintentar
            try:
                os.remove(parcial)
            except OSError:
                pass
            return "desaparecido"
        except Exception as e:
            escribir(f"  intento {intento}/{REINTENTOS} fallo ({remoto}): {e}")
            time.sleep(3)
            try:
                cerrar(conn)
                conn[:] = conectar()
            except Exception as e2:
                escribir(f"  no se pudo reconectar: {e2}")
    return "fallido"


def worker(num, cola, total):
    try:
        conn = conectar_reintentos(f"hilo {num}")
    except Exception as e:
        escribir(f"[hilo {num}] no pudo conectar, se omite este hilo: {e}")
        return
    escribir(f"[hilo {num}] conectado")
    try:
        while not stop.is_set():
            try:
                remoto, tam, mtime = cola.get_nowait()
            except queue.Empty:
                break
            estado = descargar(conn, remoto, tam, mtime)
            with lock:
                cont[estado] += 1
                cont["hecho"] += 1
                cont["bytes"] += tam if estado in ("copiado", "omitido") else 0
                n = cont["hecho"]
            if estado != "omitido":
                escribir(f"[{n}/{total}] [hilo {num}] {estado.upper()}: {remoto} ({fmt(tam)})")
    finally:
        cerrar(conn)


def main():
    libre = shutil.disk_usage(RUTA_DESTINO).free / 1024 ** 3
    escribir(f"Espacio libre en destino: {libre:.1f} GB")

    archivos = None
    if USAR_CACHE and LIMITE == 0 and os.path.exists(CACHE):
        try:
            with open(CACHE, encoding="utf-8") as fc:
                datos = json.load(fc)
            if datos.get("ruta") == RUTA_REMOTA:
                if datos.get("version") != 2:
                    raise ValueError("cache antiguo sin fechas, se vuelve a listar")
                archivos = [tuple(x) for x in datos["archivos"]]
                escribir(f"Listado cargado desde {CACHE} (borra ese archivo para volver a listar)")
        except Exception as e:
            escribir(f"No se pudo leer el cache: {e}")

    if archivos is None:
        if LIMITE > 0:
            conn = conectar()
            escribir(f"Conectado a {SFTP_HOST}:{SFTP_PORT}. Listando {RUTA_REMOTA} ...")
            archivos = list(islice(listar(conn[1], RUTA_REMOTA), LIMITE))
            escribir(f"MODO PRUEBA: solo {len(archivos)} archivos (LIMITE_ARCHIVOS={LIMITE})")
            cerrar(conn)
        else:
            archivos, errores = listar_paralelo(RUTA_REMOTA)
            if errores:
                escribir(f"ADVERTENCIA: {len(errores)} carpetas no se pudieron listar "
                         f"(no se guarda el cache). Primeras: {errores[:5]}")
            elif USAR_CACHE:
                with open(CACHE, "w", encoding="utf-8") as fc:
                    json.dump({"version": 2, "ruta": RUTA_REMOTA, "archivos": archivos}, fc)
                escribir(f"Listado guardado en {CACHE}")

    total_bytes = sum(a[1] for a in archivos)
    escribir(f"{len(archivos)} archivos a descargar, {fmt(total_bytes)}")

    if total_bytes / 1024 ** 3 + 1 > libre:
        escribir("ERROR: no hay espacio suficiente en destino.")
        return

    # Los archivos grandes primero: reparte mejor la carga entre hilos
    if ORDEN == "grandes":
        archivos.sort(key=lambda x: x[1], reverse=True)
        escribir("Orden: archivos grandes primero")
    else:
        archivos.sort(key=lambda x: x[2])   # fecha de modificacion, el mas viejo primero
        ahora = time.time()
        edades = [(ahora - a[2]) / 86400 for a in archivos]
        if edades:
            riesgo = sum(1 for e in edades if e >= DIAS_RETENCION - 3)
            escribir(f"Orden: mas antiguos primero (el mas viejo tiene {max(edades):.1f} dias, "
                     f"el mas nuevo {min(edades):.1f} dias)")
            escribir(f"Archivos con {DIAS_RETENCION - 3}+ dias (en riesgo de borrado a los "
                     f"{DIAS_RETENCION}): {riesgo}")
    cola = queue.Queue()
    for a in archivos:
        cola.put(a)

    n_hilos = min(HILOS, max(1, len(archivos)))
    escribir(f"Descargando con {n_hilos} conexiones en paralelo\n")
    inicio = time.time()

    hilos = [threading.Thread(target=worker, args=(i + 1, cola, len(archivos)), daemon=True)
             for i in range(n_hilos)]
    for h in hilos:
        h.start()
        time.sleep(PAUSA_CONEXION)
    ult_t, ult_c = time.time(), 0
    try:
        while any(h.is_alive() for h in hilos):
            time.sleep(0.5)
            if time.time() - ult_t >= 30:
                with lock:
                    c, h_ = cont["copiado"], cont["hecho"]
                vel = (c - ult_c) / (time.time() - ult_t)
                faltan = len(archivos) - h_
                eta = f"{faltan / vel / 60:.0f} min" if vel > 0 else "?"
                escribir(f"--- Progreso: {h_}/{len(archivos)} | {vel:.1f} archivos/s | faltan ~{eta}")
                ult_t, ult_c = time.time(), c
    except KeyboardInterrupt:
        escribir("\nInterrumpido por el usuario, cerrando hilos...")
        stop.set()
        for h in hilos:
            h.join(timeout=10)

    seg = max(time.time() - inicio, 1)
    escribir("\n" + "=" * 60)
    escribir(f"Copiados: {cont['copiado']} | Omitidos: {cont['omitido']} | Fallidos: {cont['fallido']} | "
             f"Borrados en origen: {cont['desaparecido']}")
    escribir(f"Descargado: {fmt(cont['bytes'])} en {seg / 60:.1f} min "
             f"({fmt(cont['bytes'] / seg)}/s)")
    escribir("=" * 60)


if __name__ == "__main__":
    try:
        main()
    finally:
        log.close()