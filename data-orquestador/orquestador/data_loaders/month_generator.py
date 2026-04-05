if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def generate_months(*args, **kwargs):
    child_data = []      # The positional arguments passed to downstream blocks
    child_metadata = []  # The kwargs (including block names) passed downstream

    starting_year = int(kwargs.get('year', 2015))
    finishing_year = min(starting_year + 4, 2026)
    
    for year in range(starting_year, finishing_year):
        for month in range(1, 2):              
            child_data.append(f"{year}-{month:02d}")
            
            # The keys in this dictionary will become kwargs in your load_data block
            child_metadata.append({
                "block_uuid": f"extract_taxi_{year}_{month:02d}", # Names the dynamic block in the UI
                "year": year,
                "end_year": finishing_year,
                "month": month
            })
            
    # A dynamic block must return a list containing the data and the metadata
    return [
        child_data,
        child_metadata
    ]