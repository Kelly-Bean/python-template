from option_data_research.gcs import list_buckets


def test_list_buckets():
    buckets = list_buckets()
    assert buckets
