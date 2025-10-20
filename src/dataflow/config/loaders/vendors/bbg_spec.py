import yaml

with open(r"../../specs/vendors/bbg_spec.yaml", 'r') as f:
    bbg_spec = yaml.load(f)
    print(bbg_spec)