import json

def recursive_find_and_replace(json, placeholder, replace):
    for key,value in json.items():
        if value == placeholder:
            json[key] = replace
        elif isinstance(json[key], dict):
            recursive_find_and_replace(json[key], placeholder, replace)

def read(file_name):
    # We need special handling of triple quoted strings for script bodies which are
    # not supported in the JSON reader. These just need to be copied as strings into
    # the request body for the aggregation. We also drop painless drop comments which
    # are prefixed by //.

    scripted_metric = ''
    with open(file_name, 'r') as file:
        for line in file.readlines():
            comment = line.find('//')
            if comment != -1:
                scripted_metric += line[:comment]
            else:
                scripted_metric += line

    split_scripted_metric = scripted_metric.split('"""')

    scripts_indices = range(1, len(split_scripted_metric), 2)

    scripts = [split_scripted_metric[i] for i in scripts_indices]
    pos = 0
    for i in scripts_indices:
        split_scripted_metric[i] = '"place_holder_' + str(pos) + '"'
        scripts[pos] = ' '.join(scripts[pos].split())
        pos = pos + 1

    scripted_metric = ''.join(split_scripted_metric)

    scripted_metric_json = json.loads(scripted_metric)

    for i in range(0, len(scripts)):
        recursive_find_and_replace(scripted_metric_json, 'place_holder_' + str(i), scripts[i])

    return scripted_metric_json
