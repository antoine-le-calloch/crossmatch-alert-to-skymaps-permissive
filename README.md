<h1>
    <img src="images/nasa_logo.png" alt="SkyPortal Logo" width="95px" align="right"/>
    <img src="images/skyportal_logo.png" alt="SkyPortal Logo" width="80px" align="right"/>
    <img src="images/boom_logo.png" alt="BOOM logo" width="80px" align="right"/>
    <br/><br/>
    Boom Gcn Pipeline
</h1>

A Python service that consumes alerts from the [BOOM/Babamul](https://babamul.caltech.edu/) broker, crossmatches them with recent GCN skymaps from [SkyPortal](https://skyportal.io/), and republishes the matches as [BOOM GCN Notices](https://gcn.nasa.gov/notices).

## About BOOM

The Burst & Outburst Observations Monitor (BOOM) is a community-facing alert and data infrastructure for time-domain and multi-messenger astronomy. It powers the Babamul multi-survey broker, which ingests and combines alerts from the Zwicky Transient Facility (ZTF) and the Vera C. Rubin Observatory to produce unified light curves, contextual metadata, and filtered event streams.

This service contributes to the BOOM GCN Notices stream: it crossmatches candidates from the BOOM [fast transient](https://github.com/boom-astro/boom-analysis-tools/blob/main/filters/fast_transient_ztf.json) and [galactic fast transient](https://github.com/boom-astro/boom-analysis-tools/blob/main/filters/galactic_fast_transient_ztf.json) filters with GW alert skymaps (BNS / NSBH mergers) and GRB localizations (Fermi, SVOM, Einstein Probe), enabling rapid cross-identification across experiments. Notices include localization, photometry, cross-matches, and ML classification scores, and are published on the GCN Kafka topic `gcn.notices.boom`.

## What it does

- Polls SkyPortal for recent GCN events with a `< 1000 sq. deg.` localization and builds MOCs from their skymaps.
- Consumes alerts from configured BOOM Kafka filters, filters photometry (public, SNR, detections / last non-detection), and checks spatial and temporal containment against each skymap.
- For every match, produces a BOOM GCN Notice to GCN Kafka (test or production). A Slack summary with the payload JSON and a PNG of the object over each matching skymap MOC can optionally be sent.
- Emits periodic heartbeats on the `gcn.notices.boom.heartbeat` topic.

A `display_skymaps(obj, skymaps, plot=False)` helper is also available in `utils/skymap.py` to log (and optionally plot with matplotlib) the skymaps that match a given object.

## Setup
```bash
git clone https://github.com/antoine-le-calloch/crossmatch-alert-to-skymaps.git
cd crossmatch-alert-to-skymaps
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.default .env
```

## Configuration
Edit the `.env` file to set your configuration.

**SkyPortal**
- `SKYPORTAL_URL`: URL of your SkyPortal instance.
- `SKYPORTAL_API_KEY`: SkyPortal API key.

**BOOM Kafka (input alerts)**
- `BOOM_KAFKA_SERVER`: Kafka broker URL (e.g. `kaboom.caltech.edu:9093` or `babamul.umn.edu:9093`).
- `BOOM_KAFKA_USERNAME` / `BOOM_KAFKA_PASSWORD`: credentials.
- `BOOM_KAFKA_TOPIC`: topic to consume from (e.g. `ZTF_alerts_results`).
- `BOOM_KAFKA_FILTERS`: comma-separated BOOM filter names to keep (e.g. `public_fast_transient_ztf,public_galactic_fast_transient_ztf`).

**GCN Kafka (output notices)**
- `GCN_KAFKA_TESTING_MODE`: `true` to publish on the test stream, `false` for production.
- `GCN_KAFKA_SERVER` / `GCN_KAFKA_USERNAME` / `GCN_KAFKA_PASSWORD` / `GCN_KAFKA_TOPIC`: production settings (`gcn.nasa.gov`, `gcn.notices.boom`).
- `GCN_KAFKA_TEST_SERVER` / `GCN_KAFKA_TEST_USERNAME` / `GCN_KAFKA_TEST_PASSWORD` / `GCN_KAFKA_TEST_TOPIC`: test settings (`test.gcn.nasa.gov`, `gcn.notices.boom.test`).

**Slack (optional)**
- `NOTIFY_SLACK`: set to `true` to enable Slack notifications.
- `SLACK_BOT_TOKEN`: bot token.
- `SLACK_CHANNEL_NAME`: channel to post in.

The BOOM GCN schema and example JSON messages are available in the [GCN Schema project](https://github.com/nasa-gcn/gcn-schema/tree/main/gcn/notices/boom).

## Running the Service
```bash
python boom_gcn_pipeline.py
```

CLI options:
- `--gcn / -g`: GCN event lookback window in hours (default: 144, i.e. 6 days).
- `--detection / -d`: first-detection fallback in hours, used to discard stale candidates (default: 120, i.e. 5 days).
- `--sleep-time / -s`: seconds between SkyPortal polls (default: 20).
- `--clean-slack / -cs`: delete all existing bot messages from the Slack channel before starting.

### Consuming the published notices

A small helper script subscribes to the BOOM GCN Kafka topics and prints incoming notices:

```bash
python consume_boom_gcn_notices.py
```

## Acknowledgments

The Babamul alerts broker and BOOM software infrastructure (du Laz et al. 2026) is co-developed by the California Institute of Technology and the University of Minnesota. This work acknowledges support from the National Science Foundation through AST Award No. 2432476 (PI Kasliwal; co-PI Coughlin) and leverages experience from the Zwicky Transient Facility (co-PIs Graham and Kasliwal).
