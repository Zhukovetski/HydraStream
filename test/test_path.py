from hydrastream.loader import HydraStream


def test_loader_creates_directories(tmp_path: str) -> None:
    """We check that the loader creates the folder structure correctly"""

    loader = HydraStream(output_dir=str(tmp_path), quiet=True)

    assert loader.storage.out_dir.exists()
    assert loader.storage.state_dir.exists()
    assert loader.storage.state_dir.name == ".states"
