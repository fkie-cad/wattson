from typing import Optional, Callable

from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger


class WattsonPostfixService(WattsonService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.logger = get_logger("Mail Server", "Mail Server")

    def start(self):
        super().start()
        ip = self._service_configuration.get("ip", "172.16.10.5")
        self.network_node.exec(f"echo nameserver {ip} > /etc/resolv.conf", shell=True)
        self.network_node.exec("cp /etc/resolv.conf /var/spool/postfix/etc/resolv.conf", shell=True)
        self.network_node.exec("chmod 1777 /etc/resolv.conf", shell=True)
        self.network_node.exec("chmod -R 1777 /var/spool/postfix/etc", shell=True)
        self.logger.info("starting mariadb")
        self.network_node.exec("service mariadb start", shell=True)
        self.logger.info("Importing old database")
        self.network_node.exec("mysql -e 'create database vmail';", shell=True, use_shlex=False)
        self.network_node.exec(f"mysql -u root --password=vmaildbpass -D vmail < /etc/postfix/sql/vmail.sql",
                               shell=True)
        self.network_node.exec(f"mysql -u root --password=vmaildbpass -D mysql < /etc/postfix/sql/mysql.sql",
                               shell=True)
        self.network_node.exec(f"mysql -e 'flush privileges';", shell=True, use_shlex=False)
        self.logger.info("Starting dovecot service")
        self.network_node.exec("chown vmail:dovecot /var/log/dovecot.log", shell=True)
        self.network_node.exec("chmod 660 /var/log/dovecot.log", shell=True)
        self.network_node.exec("service dovecot start", shell=True)
        self.logger.info("Starting postfix service")
        p = self.network_node.popen("postfix start", shell=True)
        # TODO: postfix start ends after starting postfix
        # so the service is not listed as running when it is
        self._process = p
        return True  # p.poll() is None

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False,
             async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        self.network_node.exec(["postfix", "stop"])
        return success

    def get_stderr(self):
        return self.get_stdout()

    def get_stdout(self):
        return self.get_log_handle()
