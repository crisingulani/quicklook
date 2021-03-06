from dos_monitor import DOSmonitor
from qlf_models import QLFModels
from time import sleep
from multiprocessing import Process, Event
import logging
import Pyro4
import configparser
import sys
import os
from tasks import start

qlf_root = os.getenv('QLF_ROOT')
cfg = configparser.ConfigParser()

try:
    cfg.read('%s/qlf/config/qlf.cfg' % qlf_root)
    logfile = cfg.get("main", "logfile")
    loglevel = cfg.get("main", "loglevel")
    parallel_ingestion = cfg.getboolean("main", "parallel_ingestion")
except Exception as error:
    print(error)
    print("Error reading  %s/qlf/config/qlf.cfg" % qlf_root)
    sys.exit(1)

# if parallel_ingestion:
#     from qlf_pipeline import JobsParallelIngestion as QLFPipeline
# else:
#     from qlf_pipeline import Jobs as QLFPipeline

logging.basicConfig(
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=logfile,
    level=eval("logging.%s" % loglevel)
)

logger = logging.getLogger(__name__)


class QLFAutoRun(Process):

    def __init__(self):
        super().__init__()
        self.running = Event()
        self.exit = Event()
        self.dos_monitor = DOSmonitor()
        self.last_night = str()
        self.current_exposure = None

        exposure = QLFModels().get_last_exposure()

        if exposure:
            self.last_night = exposure.night

    def run(self):
        self.clear()

        while not self.exit.is_set():
            night = self.dos_monitor.get_last_night()

            if night == self.last_night:
                logger.info("The night %s has already been processed" % night)
                sleep(10)
                continue

            exposures = self.dos_monitor.get_exposures_by_night(night)

            if not exposures:
                logger.warn('No exposure was found')
                sleep(10)
                continue

            for exposure in exposures:
                if self.exit.is_set():
                    logger.info('Execution stopped')
                    break

                self.running.set()
                self.current_exposure = exposure

                self.result = start.s(self.current_exposure).apply_async()

                # ql = QLFPipeline(self.current_exposure)
                # logger.info('Executing expid %s...' % exposure.get('expid'))
                # ql.start_process()
                # ql.start_jobs()
                # ql.finish_process()

            self.running.clear()
            self.current_exposure
            self.last_night = night

        logger.info("Bye!")

    def clear(self):
        self.exit.clear()

    def shutdown(self):
        self.exit.set()


class QLFManualRun(Process):

    def __init__(self, exposures):

        # exposures = [{
        #     'exptime': 1000, 'teldec': 0.0, 'night': '20170428', 'telra': 0.0,
        #     'desi_spectro_data': '/home/singulani/quicklook/rawdir/data', 'dateobs': '2017-04-28 22:00', 'expid': '3',
        #     'flavor': 'dark', 'zfill': '00000003', 'tile': 2000, 'cameras': [{'name': 'r7'}, {'name': 'r8'}]
        # }]

        super().__init__()
        self.running = Event()
        self.exit = Event()
        self.current_exposure = None

        # TODO: improve the method for obtaining exposures
        dos_monitor = DOSmonitor()
        night = dos_monitor.get_last_night()
        self.exposures = list()

        for exposure in exposures:
            self.exposures.append(dos_monitor.get_exposure(night, exposure))

    def run(self):
        self.exit.clear()

        logger.info(self.exposures)

        for exposure in self.exposures:
            logger.info("Initiating {} exposure processing...".format(exposure.get("expid")))

            # if exit is set
            if self.exit.is_set():
                logger.info('Execution stopped')
                break

            # is running
            self.running.set()
            self.current_exposure = exposure
            ql = QLFPipeline(self.current_exposure)
            logger.info('Executing expid {}...'.format(exposure.get('expid')))
            ql.start_process()
            ql.start_jobs()
            ql.finish_process()

        # not running
        self.running.clear()
        self.current_exposure = None

        logger.info("Bye!")
        self.shutdown()

    def clear(self):
        self.exit.clear()

    def shutdown(self):
        self.exit.set()


@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class QLFAutomatic(object):
    def __init__(self):
        self.process = False

    def start(self):
        if self.process and self.process.is_alive():
            self.process.clear()
            logger.info("Monitor is already initialized (pid: %i)." % self.process.pid)
        else:
            self.process = QLFAutoRun()
            self.process.start()
            logger.info("Starting pid %i..." % self.process.pid)

    def stop(self):
        if self.process and self.process.is_alive():
            logger.info("Stop pid %i" % self.process.pid)
            self.process.shutdown()
        else:
            logger.info("Monitor is not initialized.")

    def restart(self):
        self.stop()
        logger.info("Restarting...")
        sleep(5)
        self.start()

    def get_status(self):
        status = False

        if self.process and not self.process.exit.is_set():
            status = True

        logger.info("QLF Daemon status: {}".format(status))
        return status

    def get_current_run(self):
        return self.process.current_exposure

    def is_running(self):
        running = False

        if self.process and self.process.running.is_set():
            running = True

        logger.info("Running? {}".format(running))
        return running


@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class QLFManual(object):

    def __init__(self):
        self.process = False
        self.exposures = list()

    def start(self, exposures):
        if self.process and self.process.is_alive():
            self.process.clear()
            logger.info("Monitor is already initialized (pid: %i)." % self.process.pid)
        else:
            self.process = QLFManualRun(exposures)
            self.process.start()
            logger.info("Starting pid %i..." % self.process.pid)

    def stop(self):
        if self.process and self.process.is_alive():
            logger.info("Stop pid %i" % self.process.pid)
            self.process.shutdown()
        else:
            logger.info("Monitor is not initialized.")

    def get_status(self):
        status = False

        if self.process and not self.process.exit.is_set():
            status = True

        logger.info("QLF Manual status: {}".format(status))
        return status

    def get_current_run(self):
        return self.process.current_exposure


def main():
    try:
        auto_mode = os.environ.get('QLF_DAEMON_NS', 'qlf.daemon')
        manual_mode = os.environ.get('QLF_MANUAL_NS', 'qlf.manual')
        host = os.environ.get('QLF_DAEMON_HOST', 'localhost')
        port = int(os.environ.get('QLF_DAEMON_PORT', '56005'))
    except Exception as err:
        logger.error(err)
        sys.exit(1)

    Pyro4.Daemon.serveSimple(
        {QLFAutomatic: auto_mode, QLFManual: manual_mode},
        host=host,
        port=port,
        ns=False
    )


if __name__ == "__main__":
    main()
