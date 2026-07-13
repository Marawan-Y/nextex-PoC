# Using a real fabric/textile defect dataset

By default, `edge-simulator` generates synthetic fabric frames on the fly (see `edge_simulator/dataset.py`) — no setup required, `docker compose up` and it works immediately.

To use a real dataset instead, drop labeled images into:

```
edge_simulator/data/fabric_images/<class_name>/<image>.jpg
```

The folder name for each image becomes its ground-truth label, e.g.:

```
edge_simulator/data/fabric_images/
├── no_defect/
│   ├── img001.jpg
│   └── img002.jpg
├── needle_line/
│   └── img003.jpg
└── oil_stain/
    └── img004.jpg
```

`FrameSource` (`edge_simulator/dataset.py`) automatically detects any images under this directory on startup and switches from the synthetic generator to the real dataset — no code changes, no config flag, just restart the `edge-simulator` service (or re-run `docker compose up --build edge-simulator`).

## Suggested public datasets

Any of these work well; pick whichever has classes closest to the ones referenced in the founder's video (needle_line, horizontal_distortion, oil_stain, stitch_irregularity, hole):

- **AITEX Fabric Image Database** — a well-known public textile-defect benchmark with 7 fabric types and multiple defect categories.
- Kaggle: search **"fabric defect dataset"** or **"textile defect classification"** — several community-uploaded sets with folder-per-class structure that drops straight into the layout above.

Example using the Kaggle CLI (`pip install kaggle`, with a Kaggle API token configured at `~/.kaggle/kaggle.json`):

```bash
kaggle datasets download -d <dataset-owner>/<dataset-slug> -p edge_simulator/data/fabric_images --unzip
```

After downloading, you may need to reorganize into the `<class_name>/<image>` layout shown above if the dataset ships with a different structure (e.g. a CSV label file instead of folder-per-class) — a short one-off script, not a code change to the simulator itself.

## Why class names matter

The label (folder name) is fed into `MockDetector.detect()` as the frame's ground truth, so the mocked model can behave realistically (usually correct, occasionally wrong — see `edge_simulator/mock_detector.py`). Folder names don't need to exactly match `DEFECT_CLASSES` in `dataset.py` — anything not in that list is treated as `no_defect` — but matching them (`needle_line`, `horizontal_distortion`, `oil_stain`, `stitch_irregularity`, `hole`, `no_defect`) gives the most realistic demo.
