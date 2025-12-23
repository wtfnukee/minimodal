"""
Example: Using Cron Jobs for Scheduled Execution

Cron jobs allow you to schedule functions to run automatically at
specified intervals, either using cron expressions or simple periods.

To run this example:

1. Start the server:
   uv run python -m minimodal.server.api

2. Start a worker (in another terminal):
   uv run python -m minimodal.worker.executor

3. Run this example:
   uv run python examples/cron_jobs_example.py
"""

from datetime import datetime, timezone

from minimodal import App, Cron, Period

# Create an app
app = App("cron-example")


# Function scheduled with a cron expression
# This would run every hour at minute 0
@app.function(schedule=Cron("0 * * * *"))
def hourly_report():
    """
    Generate an hourly report.

    This function runs every hour at minute 0.
    Cron expression: "0 * * * *"
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    return {
        "report_type": "hourly",
        "generated_at": timestamp,
        "data": {"visitors": 1234, "conversions": 56},
    }


# Function scheduled with a period
# This would run every 5 minutes
@app.function(schedule=Period(minutes=5))
def health_check():
    """
    Perform a health check every 5 minutes.

    Period scheduling is simpler than cron for basic intervals.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": {
            "database": "ok",
            "cache": "ok",
            "external_api": "ok",
        },
    }


# Daily cleanup job using cron
@app.function(schedule=Cron("0 0 * * *"))
def daily_cleanup():
    """
    Run daily cleanup at midnight UTC.

    Cron expression: "0 0 * * *" = minute 0, hour 0, any day
    """
    return {
        "job": "daily_cleanup",
        "cleaned_items": 42,
        "freed_space_mb": 1024,
    }


# Weekly summary job
@app.function(schedule=Cron("0 9 * * 1"))
def weekly_summary():
    """
    Generate weekly summary every Monday at 9 AM UTC.

    Cron expression: "0 9 * * 1"
    - minute: 0
    - hour: 9
    - day of month: * (any)
    - month: * (any)
    - day of week: 1 (Monday)
    """
    return {
        "report_type": "weekly",
        "week_ending": datetime.now(timezone.utc).date().isoformat(),
        "summary": {
            "total_jobs_run": 168,  # 24 * 7 hourly jobs
            "total_health_checks": 2016,  # 12 * 24 * 7 (every 5 min)
            "errors": 0,
        },
    }


def demonstrate_cron_period_classes():
    """Demonstrate Cron and Period classes."""
    print("=" * 60)
    print("Cron and Period Classes")
    print("=" * 60)

    # Cron examples
    print("\n1. Cron Expressions:")

    cron_examples = [
        ("0 * * * *", "Every hour at minute 0"),
        ("*/15 * * * *", "Every 15 minutes"),
        ("0 0 * * *", "Daily at midnight"),
        ("0 9 * * 1-5", "Weekdays at 9 AM"),
        ("0 0 1 * *", "First day of each month"),
    ]

    for expr, description in cron_examples:
        cron = Cron(expr)
        print(f"   {cron} - {description}")

    # Period examples
    print("\n2. Period Scheduling:")

    period_examples = [
        (Period(seconds=30), "Every 30 seconds"),
        (Period(minutes=5), "Every 5 minutes"),
        (Period(hours=1), "Every hour"),
        (Period(hours=2, minutes=30), "Every 2.5 hours"),
    ]

    for period, description in period_examples:
        print(f"   {period} ({period.total_seconds}s) - {description}")

    print("\n" + "=" * 60)


def demonstrate_schedule_registration():
    """Show how schedules are registered."""
    print("\n" + "=" * 60)
    print("Schedule Registration")
    print("=" * 60)

    print("\n1. Functions with schedules:")
    for name, func in app.functions.items():
        schedule = getattr(func, "_schedule", None)
        if schedule:
            if isinstance(schedule, Cron):
                print(f"   - {name}: Cron({schedule.expression!r})")
            elif isinstance(schedule, Period):
                print(f"   - {name}: Period({schedule.total_seconds}s)")

    print("\n" + "=" * 60)


def demonstrate_scheduler_api():
    """Demonstrate the Scheduler API."""
    print("\n" + "=" * 60)
    print("Scheduler API Demonstration")
    print("=" * 60)

    from minimodal import get_client

    client = get_client()

    # List schedules
    print("\n1. Listing schedules...")
    try:
        response = client._request("GET", "/schedules")
        data = response.json()
        schedules = data.get("schedules", [])
        if schedules:
            for sched in schedules:
                print(f"   - Function ID: {sched['function_id']}")
                if sched.get("cron_expression"):
                    print(f"     Cron: {sched['cron_expression']}")
                if sched.get("period_seconds"):
                    print(f"     Period: {sched['period_seconds']}s")
                print(f"     Next run: {sched['next_run']}")
                print(f"     Enabled: {sched['enabled']}")
        else:
            print("   No schedules registered yet.")
    except Exception as e:
        print(f"   Error listing schedules: {e}")

    print("\n" + "=" * 60)


def demonstrate_next_run_calculation():
    """Show how next run times are calculated."""
    print("\n" + "=" * 60)
    print("Next Run Calculation")
    print("=" * 60)

    from minimodal.server.cron_scheduler import calculate_initial_next_run

    now = datetime.now(timezone.utc)
    print(f"\nCurrent time (UTC): {now.isoformat()}")

    # Cron next run
    print("\n1. Cron expression next runs:")
    cron_expressions = [
        "0 * * * *",  # Every hour
        "*/5 * * * *",  # Every 5 minutes
        "0 0 * * *",  # Daily at midnight
    ]

    for expr in cron_expressions:
        next_run = calculate_initial_next_run(cron_expression=expr)
        print(f"   {expr}: {next_run.isoformat()}")

    # Period next run
    print("\n2. Period next runs:")
    periods = [60, 300, 3600]  # 1 min, 5 min, 1 hour

    for period in periods:
        next_run = calculate_initial_next_run(period_seconds=period)
        print(f"   Every {period}s: {next_run.isoformat()}")

    print("\n" + "=" * 60)


def test_local_execution():
    """Test scheduled functions locally (without server)."""
    print("\n" + "=" * 60)
    print("Local Function Testing")
    print("=" * 60)

    # Test hourly_report
    print("\n1. Testing hourly_report locally...")
    result = hourly_report()
    print(f"   Result: {result}")

    # Test health_check
    print("\n2. Testing health_check locally...")
    result = health_check()
    print(f"   Result: {result}")

    # Test daily_cleanup
    print("\n3. Testing daily_cleanup locally...")
    result = daily_cleanup()
    print(f"   Result: {result}")

    print("\n" + "=" * 60)


def main():
    """Run the cron jobs example."""
    print("Cron Jobs Example")
    print("-" * 40)

    # Demonstrate Cron and Period classes
    demonstrate_cron_period_classes()

    # Show schedule registration
    demonstrate_schedule_registration()

    # Show next run calculation
    demonstrate_next_run_calculation()

    # Test functions locally
    test_local_execution()

    print("\n" + "=" * 60)
    print("Deploying Functions with Schedules")
    print("=" * 60)

    # Deploy functions
    print("\nDeploying functions...")
    app.deploy()

    # Show scheduler API
    demonstrate_scheduler_api()

    print("\n" + "=" * 60)
    print("Cron Expression Reference")
    print("=" * 60)
    print("""
    Cron expressions have 5 fields:

    ┌───────────── minute (0 - 59)
    │ ┌───────────── hour (0 - 23)
    │ │ ┌───────────── day of month (1 - 31)
    │ │ │ ┌───────────── month (1 - 12)
    │ │ │ │ ┌───────────── day of week (0 - 6, Sunday=0)
    │ │ │ │ │
    * * * * *

    Special characters:
    * : any value
    , : value list (e.g., 1,3,5)
    - : range (e.g., 1-5)
    / : step (e.g., */15 = every 15)

    Examples:
    "0 * * * *"     - Every hour at minute 0
    "*/15 * * * *"  - Every 15 minutes
    "0 9 * * *"     - Daily at 9:00 AM
    "0 9 * * 1-5"   - Weekdays at 9:00 AM
    "0 0 1 * *"     - First of each month at midnight
    """)

    print("\nNote: The cron scheduler runs as a background thread on the server.")
    print("When enabled, it will automatically trigger function invocations")
    print("at the scheduled times.")


if __name__ == "__main__":
    main()
