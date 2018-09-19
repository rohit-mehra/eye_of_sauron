import numpy as np
import base64

def np_to_json(obj):
    """Searialize numpy.ndarray obj"""
    return {'frame': base64.b64encode(obj.tostring()).decode("utf-8"),
            'dtype': obj.dtype.str,
            'shape': obj.shape}

def np_from_json(obj):
    """Desearialize numpy.ndarray obj"""
    return np.frombuffer(base64.b64decode(obj['frame'].encode("utf-8")), 
                         dtype=np.dtype(obj['dtype'])
                        ).reshape(obj['shape'])