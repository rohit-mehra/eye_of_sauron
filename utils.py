import numpy as np
import base64

def np_to_json(obj):
    """Searialize numpy.ndarray obj"""
    return {'__ndarray__': base64.b64encode(obj.tostring()),
            'dtype': obj.dtype.str,
            'shape': obj.shape}

def np_from_json(obj):
    """Desearialize numpy.ndarray obj"""
    return np.frombuffer(base64.b64decode(obj['__ndarray__']), 
                         dtype=np.dtype(obj['dtype'])
                        ).reshape(obj['shape'])