import yaml

with open(r"../specs/bbg_spec.yaml", 'r') as f:
    bbg_spec = yaml.load(f)
    print(bbg_spec)