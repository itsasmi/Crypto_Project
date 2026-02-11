import azure.functions as func
import logging
import azure.durable_functions as df
# Initialize Function App
# app = func.FunctionApp()
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS )

# Import orchestrators (decorators register them)
from timer_orchestrator import bp as timer_orchestrator_bp
from symbol_orchestrator import bp as symbol_orchestrator
from blueprints.http_manual_trigger import bp as http_bp
from hybrid_orchestrator import bp as hybrid_bp
# Register blueprints
from blueprints.daily_timer_trigger import bp as daily_timer_bp
from blueprints.activities.fetch_and_write_activity import bp as fetch_and_write_bp
from blueprints.activities.get_last_timestamp_activity import bp as get_last_ts_bp
from blueprints.activities.update_tracking_activity import bp as update_tracking_bp
from blueprints.activities.resume_synapse_activity import bp as resume_synapse_bp
from blueprints.activities.pause_synapse_activity import bp as pause_synapse_bp
from blueprints.activities.check_synapse_ready_activity import bp as check_synapse_ready_bp

app.register_blueprint(check_synapse_ready_bp)
app.register_blueprint(http_bp)
app.register_blueprint(hybrid_bp)
app.register_blueprint(daily_timer_bp)
app.register_blueprint(fetch_and_write_bp)
app.register_blueprint(get_last_ts_bp)
app.register_blueprint(update_tracking_bp)
app.register_blueprint(resume_synapse_bp)
app.register_blueprint(pause_synapse_bp)
app.register_blueprint(timer_orchestrator_bp)
app.register_blueprint(symbol_orchestrator)
logging.info("Function App initialized with all orchestrators and blueprints.")
