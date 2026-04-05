if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

from mage_ai.orchestration.triggers.api import trigger_pipeline

@custom
def trigger(*args, **kwargs):
    current_year = int(kwargs.get('year', 2015))
    next_year = current_year + 4
    if next_year < 2026:
        try:
            print(f"🚀 Triggering the Raw layer pipeline - {next_year} to {next_year+3}...")
            trigger_pipeline(
                'raw_ingestion_pipeline',
                variables={'year': next_year},
                check_status=False,
                error_on_failure=False,
                schedule_name=f'api_raw_ingestion_{next_year}_{next_year+3}'
            )
            print(f"✅ {next_year} pipeline triggered successfully!")
        except Exception as e:
            print(f"❌ Failed to trigger {next_year} pipeline: {e}")

    else:
        print("🚀 Triggering the Clean layer pipeline...")
        try:
            trigger_pipeline(
                'clean_transformation_pipeline',
                check_status=False,
                error_on_failure=False,
                schedule_name='api_raw_to_clean_trigger'
            )
            print("✅ Clean pipeline triggered successfully!")
        except Exception as e:
            print(f"❌ Failed to trigger clean pipeline: {e}")
        
    return {}