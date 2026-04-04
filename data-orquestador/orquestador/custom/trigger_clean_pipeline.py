if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

from mage_ai.orchestration.triggers.api import trigger_pipeline

@custom
def trigger(*args, **kwargs):
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