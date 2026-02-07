FROM mcr.microsoft.com/playwright:v1.55.0-noble

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# ------------------------------------------------------------
# System packages: GUI, VNC, GTK build deps, Python deps
# ------------------------------------------------------------
RUN apt-get update && apt-get install -y \
    xfce4 xfce4-goodies \
    tightvncserver x11vnc \
    autocutsel dbus-x11 \
    xfonts-base xfonts-75dpi \
    iputils-ping netcat-openbsd \
    ca-certificates curl \
    \
    # Python system deps
    python3 python3-venv python3-pip \
    python3-tk python3-dev \
    \
    # Build / GTK deps (for clipboard-daemon)
    build-essential \
    libgtk-3-dev \
    \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# XFCE + VNC startup
# ------------------------------------------------------------
RUN mkdir -p /root/.vnc && \
    echo '#!/bin/sh\n\
export XKL_XMODMAP_DISABLE=1\n\
unset SESSION_MANAGER\n\
unset DBUS_SESSION_BUS_ADDRESS\n\
eval "$(dbus-launch --sh-syntax)"\n\
autocutsel -fork -selection PRIMARY &\n\
autocutsel -fork -selection CLIPBOARD &\n\
startxfce4 &' > /root/.vnc/xstartup && \
    chmod +x /root/.vnc/xstartup

# ------------------------------------------------------------
# Python venv
# ------------------------------------------------------------
ENV VENV_DIR=/opt/venv

RUN python3 -m venv ${VENV_DIR} && \
    ${VENV_DIR}/bin/pip install -U pip && \
    \
    # Python runtime deps
    ${VENV_DIR}/bin/pip install \
        camoufox[geoip] \
        playwright \
        pproxy \
        python-daemon \
        pyautogui \
        pillow \
    && \
    ${VENV_DIR}/bin/python -m camoufox fetch

ENV PATH="${VENV_DIR}/bin:${PATH}"

# ------------------------------------------------------------
# Runtime
# ------------------------------------------------------------
WORKDIR /app

# start.sh теперь НЕ копируем в образ
# Он будет примаплен volume’ом

EXPOSE 5903

CMD ["/start.sh"]
