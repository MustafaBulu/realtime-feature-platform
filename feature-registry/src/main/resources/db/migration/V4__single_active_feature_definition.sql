create unique index if not exists ux_feature_definitions_one_active_version
    on feature_definitions (name)
    where state = 'ACTIVE';
