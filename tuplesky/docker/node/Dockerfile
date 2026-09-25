# A Jepsen node for the TupleSky tests: Debian with sshd, and every
# package jepsen.os.debian installs already present, so a test's OS setup
# has nothing to fetch.
#
# Trixie, because coordd is built on the control host (an Ubuntu 24.04
# runner in CI) and needs a C library at least as new as that one.
ARG BASE=debian:trixie-slim
FROM ${BASE}

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openssh-server sudo ca-certificates procps \
      apt-transport-https apt-utils build-essential bzip2 curl dirmngr \
      faketime iproute2 iptables iputils-ping logrotate man-db \
      netcat-openbsd ntpsec-ntpdate psmisc rsyslog tar tcpdump unzip vim wget \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /run/sshd /root/.ssh \
 && chmod 700 /root/.ssh \
 && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D", "-e"]
