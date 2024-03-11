import threading
import logging
import signal
import os

logger = logging.getLogger(__name__)


def wait_for_keypress(event: threading.Event, key: str):
    """
    waits for the 'key' to be pressed. It returns a keypress_event queue
    that becomes non-empty when the required 'key' is pressed (and ENTER).
    """

    def key_e():
        while True:
            k = input()
            if k.lower() == key.lower():
                logger.info(f'Received key press event: "{key}"')
                event.set()
                break

    key_event = threading.Thread(target=key_e)
    # we don't want the key_event thread to block the program from exiting.
    key_event.daemon = True
    key_event.start()


def wait_for_sigusr1(event: threading.Event):
    def handler(signum, frame):
        logger.info("Received signal: SIGUSR1")
        event.set()
    signal.signal(signal.SIGUSR1, handler)


# If the process is not running on a TTY then reading from stdin will raise and
# exception. We read from stdin to start the switch-over phase. If it's not a
# TTY the switch-over can be started by sending a SIGUSR1 signal to the
# process.
IS_TTY = os.isatty(0)

def wait_for_event(key):
    event = threading.Event()
    wait_for_sigusr1(event)
    if IS_TTY:
        wait_for_keypress(event, key)

    return event
