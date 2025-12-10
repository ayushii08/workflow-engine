"""Workflow implementations and examples."""
from app.workflows.data_quality import register_data_quality_tools

# Auto-register data quality tools on import
register_data_quality_tools()
