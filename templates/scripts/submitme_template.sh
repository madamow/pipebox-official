#!/usr/bin/env bash

{% for file in args['rendered_template_path'] %}
dessubmit {{ file }} 
sleep 5 
{% endfor %}
