if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def generate_months(*args, **kwargs):
    """
    Generates a list of years and months to dynamically spawn downstream blocks.
    """
    child_data = []      # The positional arguments passed to downstream blocks
    child_metadata = []  # The kwargs (including block names) passed downstream
    
    for year in range(2015, 2016):
        for month in range(1, 4):
            # Optional: Stop if we are in the future (e.g., beyond April 2025)
            # if year == 2025 and month > 4:
            #     break
                
            child_data.append(f"{year}-{month:02d}")
            
            # The keys in this dictionary will become kwargs in your load_data block
            child_metadata.append({
                "block_uuid": f"extract_taxi_{year}_{month:02d}", # Names the dynamic block in the UI
                "year": year,
                "month": month
            })
            
    # A dynamic block must return a list containing the data and the metadata
    return [
        child_data,
        child_metadata
    ]
