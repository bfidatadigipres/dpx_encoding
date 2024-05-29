import os

# Storage paths
FILM_OPS = os.environ.get('FILM_OPS')
QNAP_FILM = os.environ.get('QNAP_FILM')
QNAP_FILMOPS = os.environ.get('QNAP_FILMOPS')
QNAP_FILMOPS2 = os.environ.get('QNAP_FILMOPS2')
DIGI_OPS = os.environ.get('IS_DIGITAL')
QNAP_DIGIOPS = os.environ.get('QNAP_DIGIOPS')
QNAP_11_DIGIOPS = os.environ.get('QNAP_11_DIGIOPS')

# Automation DPX paths
ASSESS = os.environ.get('DPX_ASSESS')
NO_GAP = os.environ.get('DPX_NO_GAP')
DPX_COOK = os.environ.get('DPX_COOK')
DPX_REVIEW = os.environ.get('DPX_REVIEW')
DPX_COMPLETE = os.environ.get('DPX_COMPLETE')
RAWCOOK = os.environ.get('RAWCOOKED_PATH')
MKV_ENCODED = os.environ.get('MKV_ENCODED')
TO_DELETE = os.environ.get('TO_DELETE')

# Documents
DPOLICY = os.environ.get('POLICY_DPX')
MPOLICY = os.environ['POLICY_RAWCOOK']
DATABASE = os.path.join(os.environ.get('LOG_PATH'), 'rawcooked_dagster.db')
LICENCE = os.environ.get('RAWCOOKED_LICENSE')
DOWNTIME = os.environ.get('DOWNTIME_CONTROL')