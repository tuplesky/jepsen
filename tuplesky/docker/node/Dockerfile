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

# With --build-arg LIBC_DEBUG=1 (docker/up.sh --libc-debug), glibc's debug
# symbols too, for profiling runs: perf then names libc's local functions
# (memcpy's and memmove's variants, _int_free), which the stripped library
# leaves as bare addresses, and can unwind through them. A layer of its
# own, after the packages every run takes.
ARG LIBC_DEBUG=
RUN if [ -n "$LIBC_DEBUG" ]; then \
      apt-get update \
      && { DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends libc6-dbg \
           || echo "libc6-dbg is not available from this base; the profile keeps libc's bare addresses"; } \
      && rm -rf /var/lib/apt/lists/*; \
    fi

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D", "-e"]
