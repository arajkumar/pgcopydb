import logging
import json
import urllib.request

SCRIPT_VERSION = "v0.0.23"

DOCKER_IMAGE_NAME = "timescale/live-migration"

logger = logging.getLogger(__name__)

def get_latest_docker_image_tag():
    """
    Uses dockerhub API to get the latest docker image tag.
    """
    # Get 1000 results ordered by last_updated
    url = f"https://hub.docker.com/v2/repositories/{DOCKER_IMAGE_NAME}/tags/?page_size=100&ordering=last_updated"
    with urllib.request.urlopen(url) as response:
        data = response.read().decode("utf-8")
        response = json.loads(data)
    # Skip all tags except the version tags.
    # Alternatively, we can filter tags which start with "v".
    result = filter(lambda r: r["name"] not in ("latest", "main", "edge"), response['results'])
    return next(result)["name"]

def nudge_user_to_update():
    try:
        latest_tag = get_latest_docker_image_tag()
    except Exception:
        # Ignore any errors in fetching the latest tag
        pass
    else:
        if latest_tag != SCRIPT_VERSION:
            logger.warning(f"A new version ({latest_tag}) of the {DOCKER_IMAGE_NAME} docker image is available.")
            logger.warning(f"Please use {DOCKER_IMAGE_NAME}:{latest_tag} for the best experience.")
            logger.warning("If you decide to upgrade, restart the migration from scratch to avoid incompatibility.")
