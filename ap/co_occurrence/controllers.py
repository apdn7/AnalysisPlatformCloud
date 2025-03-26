import os

from flask import Blueprint

from ap.common.flask_customize import render_template
from ap.common.services.form_env import get_common_config_data

co_occurrence_blueprint = Blueprint(
    'co_occurrence',
    __name__,
    template_folder=os.path.join('..', 'templates', 'co_occurrence'),
    static_folder=os.path.join('..', 'static', 'co_occurrence'),
    url_prefix='/ap',
)


@co_occurrence_blueprint.route('/cog')
def index():
    output_dict = get_common_config_data(get_visualization_config=False)
    return render_template('co_occurrence_csv.html', **output_dict)
