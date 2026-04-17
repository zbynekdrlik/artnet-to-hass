# Deploy — artnet-to-hass

## Prod target
Linux x86_64, systemd.

## First-time setup

1. On the build machine (dev):
   ```bash
   cargo build --release
   # Binary: target/release/artnet-to-hass
   ```

2. On the prod machine:
   ```bash
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin artnet
   sudo mkdir -p /opt/artnet-to-hass
   sudo chown artnet:artnet /opt/artnet-to-hass
   ```

3. Copy artifacts:
   ```bash
   scp target/release/artnet-to-hass prod:/tmp/
   scp deploy/artnet-to-hass.service prod:/tmp/
   scp .env.example prod:/tmp/
   ssh prod '
     sudo mv /tmp/artnet-to-hass /opt/artnet-to-hass/artnet-to-hass
     sudo chmod +x /opt/artnet-to-hass/artnet-to-hass
     sudo chown artnet:artnet /opt/artnet-to-hass/artnet-to-hass
     sudo cp /tmp/.env.example /opt/artnet-to-hass/.env
     sudo chown artnet:artnet /opt/artnet-to-hass/.env
     sudo chmod 600 /opt/artnet-to-hass/.env
     sudo mv /tmp/artnet-to-hass.service /etc/systemd/system/
   '
   ```

4. Edit `/opt/artnet-to-hass/.env` with the real HA_TOKEN and HA_LIGHTS.

5. Enable and start:
   ```bash
   ssh prod '
     sudo systemctl daemon-reload
     sudo systemctl enable --now artnet-to-hass
     sudo systemctl status artnet-to-hass
   '
   ```

6. Watch logs:
   ```bash
   ssh prod 'journalctl -fu artnet-to-hass'
   ```

## Updates
```bash
cargo build --release
scp target/release/artnet-to-hass prod:/tmp/
ssh prod '
  sudo systemctl stop artnet-to-hass
  sudo mv /tmp/artnet-to-hass /opt/artnet-to-hass/artnet-to-hass
  sudo systemctl start artnet-to-hass
'
```

## Firewall
UDP port 6454 must be reachable from the Kitten console's subnet.
