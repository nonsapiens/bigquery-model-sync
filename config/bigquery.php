<?php

return [
    'projectId' => env('GOOGLE_CLOUD_PROJECT'),
    'dataset' => env('BIGQUERY_DATASET'),

    /**
     * A list of namespaces to check for traited classes.
     * If the config is empty, then it is assumed App/Models is the only namespace to pay attention to.
     */
    'syncable-namespaces' => [],
    'autosync' => env('BIGQUERY_AUTOSYNC', true),
];
