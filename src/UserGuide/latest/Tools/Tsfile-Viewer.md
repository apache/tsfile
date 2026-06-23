<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# tsfile-viewer

[Apache TsFile Viewer](https://github.com/apache/tsfile-viewer) is a web-based
application for browsing and analyzing TsFile data in your browser. It pairs a
Spring Boot backend (which reads `.tsfile` files via the Apache TsFile library)
with a Vue 3 frontend that renders metadata, paginated tables, and interactive
charts.

- **Repository:** <https://github.com/apache/tsfile-viewer>
- **License:** Apache-2.0

## Features

- **File browsing and upload** — open `.tsfile` files from the UI.
- **Metadata display** — schema, devices, and measurements.
- **Paginated data tables** with filtering by time range, devices, measurements,
  and value range.
- **Interactive charts** (ECharts) with multi-series overlay and aggregation.
- **Both data models** — supports tree-model and table-model TsFiles.
- **Export** — data as CSV or JSON; charts as PNG or SVG.
- **Performance** — chunk-level reading and metadata caching.

## Requirements

| Component | Version |
|---|---|
| JDK | 17 or 21 (LTS) |
| Maven | 3.9+ |
| Node.js | `^20.19.0 \|\| >=22.12.0` |
| pnpm | latest |
| Apache TsFile | 2.3.0 (bundled dependency) |

## Get the source

Clone the repository, then build and run it as shown below:

```bash
git clone https://github.com/apache/tsfile-viewer.git
cd tsfile-viewer
```

## Running from source (development)

Run the backend and frontend in two terminals.

**Backend** (Spring Boot):

```bash
cd backend
mvn spring-boot:run
```

**Frontend** (Vue + Vite dev server):

```bash
cd frontend
pnpm install
pnpm dev
```

Then open the dev UI at <http://localhost:5173/view/>.

## Building and running a production bundle

Build a self-contained distribution, then launch the packaged jar (the frontend
is served by the backend):

```bash
./build-dist.sh
java -jar backend/target/tsfile-viewer-*.jar
```

Open the app at <http://localhost:8080/view/>.

## Usage

After opening the UI, a typical workflow is:

1. **Select a file** — browse the server's whitelisted directories in the left-hand file tree and click a `.tsfile`, or drag/select a local file into the "Upload TsFile" card; recently opened files are listed under "Recent".
2. **View metadata** — the metadata page shows the version, time range, and device/measurement counts. Use the view-mode toggle to switch between the Tree model (measurements, devices, chunks) and the Table model (a table's TAG/FIELD column definitions).
3. **Preview data** — click "Data Preview" and use the filter panel to narrow by time range, devices, measurements, and value range, with pagination (page size / offset); data is shown as a paginated table.
4. **Visualize** — plot the selected series as interactive ECharts charts with multi-series overlay, aggregation, and drill-down; large datasets are downsampled automatically.
5. **Export** — export the filtered data as CSV / JSON, or export charts as PNG / SVG.

For a full walkthrough, see the [user guide](https://github.com/apache/tsfile-viewer/blob/main/docs/USER_GUIDE.md) in the repository.
