from epic_fhir.auth import FHIRClient

client = FHIRClient()
bundle = client.get("Patient", params={"_count": 5})
entries = bundle.get("entry", [])
print(f"Connected successfully. Records returned: {len(entries)}")
print(f"First patient ID: {entries[0]['resource']['id']}")