"""Build the CodeDeploy bundle: publish-deploy/* at zip root + appspec.yml + scripts/."""
import zipfile, os, datetime, sys

timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
zip_name = f"jobworker-{timestamp}.zip"
base = os.path.dirname(os.path.abspath(__file__))
publish_path = os.path.join(base, "publish-deploy")
zip_path = os.path.join(base, zip_name)

if not os.path.isdir(publish_path):
    sys.exit("publish-deploy/ not found — run dotnet publish first")

seen = set()
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
    # app files at zip root (appspec copies \ -> C:\DCatalog\JobWorker-staging)
    for root, dirs, files in os.walk(publish_path):
        for file in files:
            file_path = os.path.join(root, file)
            arcname = os.path.relpath(file_path, publish_path).replace("\\", "/")
            zf.write(file_path, arcname)
            seen.add(arcname)
    # deployment manifest + lifecycle scripts — repo versions win unless already published
    if "appspec.yml" not in seen:
        zf.write(os.path.join(base, "appspec.yml"), "appspec.yml")
    for f in os.listdir(os.path.join(base, "scripts")):
        if f"scripts/{f}" not in seen:
            zf.write(os.path.join(base, "scripts", f), f"scripts/{f}")

size_mb = os.path.getsize(zip_path) / 1024 / 1024
print(f"Created: {zip_name}")
print(f"Size: {size_mb:.1f} MB")
print(f"Path: {zip_path}")
