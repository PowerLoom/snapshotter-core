"""
This module handles the configuration settings for the snapshotter application.
It loads and processes various configuration files, including settings, projects,
aggregators, preloaders, and delegate tasks.
"""
import json

from snapshotter.utils.models.settings_model import AggregatorConfig
from snapshotter.utils.models.settings_model import PreloaderConfig
from snapshotter.utils.models.settings_model import ProjectsConfig
from snapshotter.utils.models.settings_model import Settings

# Load main settings
with open('snapshotter/settings/settings.json', 'r') as settings_file:
    settings_dict = json.load(settings_file)
settings: Settings = Settings(**settings_dict)

# Load projects configuration
projects_config_path = settings.projects_config_path
with open(projects_config_path) as projects_config_file:
    projects_config_dict = json.load(projects_config_file)
projects_config = ProjectsConfig(**projects_config_dict).config

# Sanity check: Ensure all project types are unique
project_types = set()
for project in projects_config:
    project_types.add(project.project_type)
    project.projects = [project.lower() for project in project.projects]
assert len(project_types) == len(projects_config), 'Duplicate project types found'

# Load aggregator configuration
aggregator_config_path = settings.aggregator_config_path
with open(aggregator_config_path) as aggregator_config_file:
    aggregator_config_dict = json.load(aggregator_config_file)
aggregator_config = AggregatorConfig(**aggregator_config_dict).config

# Sanity check: Ensure all aggregator types are unique
aggregator_types = set()
for aggregator in aggregator_config:
    aggregator_types.add(aggregator.project_type)
assert len(aggregator_types) == len(aggregator_config), 'Duplicate aggregator types found'

# Ensure no overlap between project types and aggregator types
assert len(project_types & aggregator_types) == 0, 'Overlap found between project and aggregator types'

# Load preloader configuration
preloader_config_path = settings.preloader_config_path
with open(preloader_config_path) as preloader_config_file:
    preloader_config_dict = json.load(preloader_config_file)
preloader_config = PreloaderConfig(**preloader_config_dict)
preloaders = preloader_config.preloaders
delegate_tasks = preloader_config.delegate_tasks

# Sanity check: Ensure all preloader types are unique
preloader_types = set()
for preloader in preloaders:
    preloader_types.add(preloader.task_type)
assert len(preloader_types) == len(preloaders), 'Duplicate preloader types found'

# Sanity check: Ensure all delegate task types are unique
delegate_task_types = set()
for delegate_task in delegate_tasks:
    delegate_task_types.add(delegate_task.task_type)
assert len(delegate_task_types) == len(delegate_tasks), 'Duplicate delegate task types found'

# Ensure no overlap between preloader types and delegate task types
assert len(preloader_types & delegate_task_types) == 0, 'Overlap found between preloader and delegate task types'
