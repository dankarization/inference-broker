"""Preserve original catalogue fingerprints while checking explicitly authorized ceilings."""
# PR15 updates only these five existing max_context fields. Everything else
# in the original snapshot must retain its original value/fingerprint.
AUTHORIZED_CONTEXT_CHANGES = {
    'openclaw': (131_072, 262_144),
    'openclaw-qwen3-vl': (212_992, 262_144),
    'openclaw-unc-rvn-iq2xxs': (131_072, 262_144),
    'openclaw-nemotron-nano': (262_144, 1_048_576),
    'openclaw-qwen3-vl-2b': (8_192, 262_144),
}

def original_catalogue(testcase, profiles):
    historical = {name: dict(fields) for name, fields in profiles.items()}
    for name, (before, after) in AUTHORIZED_CONTEXT_CHANGES.items():
        testcase.assertEqual(historical[name]['max_context'], after,
                             'authorized live-catalogue ceiling changed: ' + name)
        historical[name]['max_context'] = before
    return historical
