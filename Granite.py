# vim:fileencoding=utf-8:ts=2:sw=2:sts=2:expandtab

import abc
import re
import time
import json
import datetime
import copy
import collections
import collections.abc
import traceback
import sys
import functools
import inspect
import importlib
import secrets

from decimal import Decimal
from json import JSONDecodeError

try:
  import rich.pretty, rich.console, rich.prompt
  CONSOLE = rich.console.Console()
  CONSOLE.pprint = rich.pretty.pprint
  CONSOLE.Prompt = rich.prompt.Prompt
  CONSOLE.Confirm = rich.prompt.Confirm
except ImportError:
  rich = None

try:
  import yaml
except ImportError:
  yaml = None

try:
  import postgresql
  import postgresql.exceptions
  import postgresql.driver
except ImportError:
  postgresql = None

try:
  import redis
except ImportError:
  redis = None

from urllib.parse import urlsplit, urlencode, urlunsplit, parse_qsl, quote_plus, unquote_plus
from xml.sax.saxutils import escape, quoteattr

def IMP(impstr):
  '''
  This is a quick and "inline" way to import 1 thing out of a module and return it
  
  Accepts a "foo.bar.baz" type string and will
  1. strip off foo.bar
  2. import it
  3. then return the baz attribute
  '''
  
  smod, _, attr = impstr.rpartition('.')
  
  mod = importlib.import_module(smod)
  
  try:
    return getattr(mod, attr)
  except AttributeError:
    raise ImportError('Could not import {} due to AttributeError'.format(impstr))


class AttributeWriteLock():
  def __setattr__(self, key, value):
    if not hasattr(type(self), key):
      raise AttributeError(f'On {type(self)}, attribute .{key} must be defined at the class level to be writable.')
    return super().__setattr__(key, value)

  def AttributeWriteLock_Override(self, key, value):
    return super().__setattr__(key, value)

class Undefined:
  '''
  Used to indicate a value that has not been defined.  Normally, None is used
  for that, but None means NULL when working with PostgreSQL, so for nullable
  fields, there is the need for a separate value.

  '''
  __slots__ = ()
  def __repr__(self):
    return "Undefined"
  def __str__(self):
    return "Undefined"
  def __bool__(self):
    return False
Undefined = Undefined()

def STRN(s, /):
  if s in (None, ''):
    return None
  else:
    return str(s)

def BOOLN(s, /):
  if s in (None, ''):
    return None
  else:
    return bool(s)

def INTN(s, /):
  if s in (None, ''):
    return None
  else:
    return int(s)

def FLOATN(s, /):
  if s in (None, ''):
    return None
  else:
    return float(s)


def DECIMALN(s, /):
  if s in (None, ''):
    return None
  else:
    return Decimal(s)


# Attribute Access Dict
class aadict(dict):
  __slots__ = ()

  def __getattr__(self, attr, /):
    try:
      return self[attr]
    except KeyError as e:
      raise AttributeError(str(e)) from None

  __setattr__ = dict.__setitem__

  __delattr__ = dict.__delitem__

  def __deepcopy__(self, memo):
    return aadict((k, copy.deepcopy(v, memo)) for k, v in self.items())

# This class is used to extend the `dict` response types with seamless attributes, by prefixing 
# __getattr__ and __setattr__ with a `.` character.   That way all __repr__ type calls will still
# show attributes
class obdict(dict):
  __slots__ = ()
  def __getattr__(self, key):
    return self['.' + key]
  def __setattr__(self, key, value):
    self['.' + key] = value
  def __delattr__(self, key):
    del self['.' + key]
  def item_items(self):
    for k, v in self.items():
      if k[0] != '.':
        yield (k[1:], v)
  def attr_items(self):
    for k, v in self.items():
      if k[0] == '.':
        yield (k[1:], v)
  def __deepcopy__(self, memo):
    return obdict((k, copy.deepcopy(v, memo)) for k, v in self.items())

# Attribute Access Dict
class ARGS(dict):
  __slots__ = ()

  def __getattr__(self, attr):
    try:
      return self[attr]
    except KeyError as e:
      raise AttributeError(str(e)) from None

  __setattr__ = dict.__setitem__

  __delattr__ = dict.__delitem__


class SQL(str):
  __slots__ = ()
  def __repr__(self):
    return 'SQL(' + str.__repr__(self) + ')'

class HTML(str):
  __slots__ = ()
  def __repr__(self):
    return 'HTML(' + str.__repr__(self) + ')'
  def __iadd__(self, other):
    return HTML(str.__add__(self, other))


def COAL(*args):
  for arg in args:
    if arg is not None:
      return arg

def STUP(value, cls=None):
  if isinstance(value, list):
    value = tuple(value)
  elif isinstance(value, tuple):
    pass
  elif value is None:
    value = ()
  else:
    value = (value,)
  if cls is not None:
    for i,v in enumerate(value):
      if not isinstance(v, cls):
        raise TypeError(f'tuple element [{i}] must be a {cls}, but is: {type(v)}')
  return value


class NULL():
  '''
  Singleton to indicate SQL NULL
  '''
  __slots__ = ()
  def __repr__(self):
    return "NULL"
  def __str__(self):
    return "NULL"
  def __bool__(self):
    return False
NULL = NULL()




###############################################################################
# JSON Support

class JSONEncoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, Decimal):
      # MUST encode decimals as str so their precision is preserved
      return str(o)
    elif isinstance(o, (datetime.datetime, datetime.date)):
      return o.isoformat()
    elif isinstance(o, set):
      return sorted(o) # keep the encoding the same from save to save
    elif isinstance(o, tuple):
      return list(o)
    return super().default(o)

class JSONDecoder(json.JSONDecoder):
  pass


_Default_Encoder = JSONEncoder(
  skipkeys=False,
  ensure_ascii=False,
  check_circular=True,
  allow_nan=True,
  indent=None,
  separators=None,
  default=None,
  )

_Default_Decoder = JSONDecoder(
  object_hook=None,
  object_pairs_hook=aadict,
  )


JSON_Encode = _Default_Encoder.encode
JSON_Decode = _Default_Decoder.decode


###############################################################################
def ML(URL, *args, _ReplaceScriptPath=None, _fragment=Undefined, **kwargs):
  # args is a list of 2-tuples (name, value) to be appended to the query string
  # kwargs is a mapping of name,value pairs which REPLACE the query string item(s) with that name
  
  url = list(urlsplit(URL))
  
  if _fragment is Undefined:
    pass
  elif _fragment is None:
    url[4] = None
  else: 
    url[4] = str(_fragment)

  qs = []
  for q in parse_qsl(url[3], keep_blank_values=True) : 
    qs.append((q[0],q[1])) 

  for v in args :
    qs.append(v) 

  # first filter out any keys from kwargs, then append the keys and values at the end
  qs = \
    [v for v in qs if v[0] not in kwargs] + \
    [(k, kwargs[k]) for k in kwargs if kwargs[k] is not None and kwargs[k] is not Undefined]

  if _ReplaceScriptPath is not None:
    url[2] = _ReplaceScriptPath

  url[3] = urlencode(qs)
  return urlunsplit(url)

###############################################################################
def SL(text, strip_prefix=None):
  '''
  SL: StripLines... strip space from beginning of lines to allow indented
  multi-line strings
  '''
  if text is None:
    return None

  lines = text.split('\n')

  if lines[0] == '':
    del lines[0]

  if len(lines) == 0:
    return ''

  if lines[-1].strip():
    raise ValueError('There must be only whitespace after last newline.')

  if strip_prefix is None:
    strip_prefix = lines[0][0:len(lines[0]) - len(lines[0].lstrip())]

  strip_len = len(strip_prefix)

  # Only strip the beginning if it is an exact match for the strip_prefix
  return str.join('\n', (line[strip_len:] if line.startswith(strip_prefix) else line for line in lines[:-1])) + '\n'


########################################################################################################################
if yaml:
  def YAML_Decode_Post(value):
    if type(value) is dict:
      return aadict((k, YAML_Decode_Post(v)) for k,v in value.items())

    elif type(value) is list:
      return [YAML_Decode_Post(v) for v in value]

    elif type(value) is set:
      return {YAML_Decode_Post(v) for v in value}
    
    else:
      return value
      

  def YAML_Encode_Prep(value):
    if isinstance(value, dict):
      return {k: YAML_Encode_Prep(v) for k,v in value.items()}

    elif isinstance(value, (tuple, list)):
      return [YAML_Encode_Prep(v) for v in value]

    elif isinstance(value, set):
      return {YAML_Encode_Prep(v) for v in value}
    
    else:
      return value
    


  try:
    from yaml import CSafeLoader
    from yaml import CSafeDumper
    def YAML_Decode(stream):
      return YAML_Decode_Post(yaml.load(stream, Loader=CSafeLoader))
    def YAML_Encode(data):
      return yaml.dump(YAML_Encode_Prep(data), sort_keys=False, Dumper=CSafeDumper)
  except ImportError:
    def YAML_Decode(stream):
      return YAML_Decode_Post(yaml.safe_load(stream))
    def YAML_Encode(data):
      return yaml.safe_dump(YAML_Encode_Prep(data), sort_keys=False)
    sys.stderr.write('WARNING - yaml not using libyaml implemenation!\n')

pass#if yaml

########################################################################################################################
class DataError(Exception):
  """
  To be raised in the event that data that is expected to be there is not found

  Internally stores error messages in
    self._error_list
  as 3 tuples of
    (FieldName or None, str(Message), Value or Undefined)

  May pass
  - a list of error messages,
  - a dict of error keys -> error messages
  - a list of 2-tuples (error key, error message)
  - a list of 3-tuples (error key, error message, input value)
  """
  def __init__(self, /, errors):
    self._error_list = []

    if isinstance(errors, dict):
      self._error_list += ((k,v,Undefined) for k,v in errors.items())

    elif isinstance(errors, (list, tuple)):
      for i, t in enumerate(errors):
        if isinstance(t, str):
          self._error_list.append((None, t, Undefined))
        elif isinstance(t, tuple) and len(t) == 2:
          self._error_list.append((t[0], t[1], Undefined))
        elif isinstance(t, tuple) and len(t) == 3:
          self._error_list.append(t)
        else:
          raise TypeError(f'Invalid type for `errors[{i}]` parameter: {t}')


    elif isinstance(errors, str):
      self._error_list.append((None, errors, Undefined))

    else:
      raise TypeError(f'Invalid type for `errors` parameter: {type(errors)}')






########################################################################################################################
class DataNotFoundError(Exception):
  """
  To be raised in the event that data that is expected to be there is not found
  """
  pass


########################################################################################################################
class DataConflictError(Exception):
  """
  To be raised in the event that data that is not expected to be there is there
  """
  pass


########################################################################################################################
class AuthorizationError(Exception):
  def __init__(self, Message, Code='', *, RedirectURI=None, Redirect=False):
    self.Message      = Message
    self.Code         = str(Code)
    self.RedirectURI  = RedirectURI
    self.Redirect     = Redirect
    super().__init__(Message)

  def __repr__(self):
    return f'AuthorizationError({repr(self.Message)}, RedirectURI={repr(self.RedirectURI)})'


########################################################################################################################

class DT():
  DefaultTimeZone=None

  @classmethod
  def UTCNow(cls):
    return datetime.datetime.now(tz=datetime.timezone.utc)

  @classmethod
  def UTCDate(cls):
    return cls.UTCNow().date();

  @classmethod
  def FormatDate(cls, DateObject, /, *, NoneText='None', TimeZone=None):
    if TimeZone is None:
      TimeZone = cls.DefaultTimeZone
    #TODO: timezone support if DateObject is a datetime object
    return DateObject.strftime('%b %d, %Y') if DateObject else NoneText

########################################################################################################################

class SEC():
  @staticmethod
  def Token16(Length):
    return secrets.token_hex(Length//2+1)[0:Length]

########################################################################################################################
class AS3():
  '''
  Some notes for future reference:
  - At this time, missing object keys are treated as None
  - What options would we want for treating them as Undefined?
  - ValidationError support is sorely missing


  '''
  @classmethod
  def Annotate(cls, fun):
    # Convert annotations to AS3 objects
    for k,v in fun.__annotations__.items():
      fun.__annotations__[k] = cls(v, StructPath=(fun.__name__, k))
  
    # Identify all paramter names
    sig = inspect.signature(fun)

    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
      bound = sig.bind(*args, **kwargs)
      for arg, as3 in fun.__annotations__.items():
        if arg == 'return':
          continue
        bound.arguments[arg] = as3(bound.arguments[arg])

      rval = fun(*bound.args, **bound.kwargs)
      
      if 'return' in fun.__annotations__:
        rval = fun.__annotations__['return'](rval)

      return rval

    return wrapper


  __slots__ = ('CompiledFunction', 'CompiledFunctionCode', 'Struct', 'StructPath')

  class CompiledCodeError(Exception):
    pass

  PythonOpt = collections.namedtuple('PythonOpt', (
    'FunctionName', 
    'InputVar',
    'OutputVar', 
    'ErrorVar', 
  ))

  def __init__(self, Struct, *, StructPath=None, Compile=True):
    if StructPath is None:
      self.StructPath = ('<Data>',)
    else:
      self.StructPath = tuple(StructPath)

    self.CompiledFunction = None
    self.CompiledFunctionCode = None
    
    self.Struct = self.Struct_(self.StructPath, Struct)
       
    
    if Compile:
      self.Compile()


  def __call__(self, Data):
    if not self.CompiledFunction:
      self.Compile()
    try:
      return self.CompiledFunction(Data)
    except Exception as e:
      tb = traceback.format_exc(limit=-1)
      raise self.CompiledCodeError(
        f'An error occured in the following code:\n\n' +
        ''.join(f'{i:4n}: {l}' for i, l in enumerate(self.CompiledFunctionCode.splitlines(keepends=True), 1)) + '\n\n' +
        str(Data) + '\n\n' +
        tb
        )

  def Compile(self):
    try:
      self.CompiledFunctionCode = '\n'.join(self.Python(FunctionName='AS3_Generated_Function'))
      l = {}
      exec(self.CompiledFunctionCode, {}, l)
      self.CompiledFunction = l['AS3_Generated_Function']
    except:
      raise

  def Python(self,
    FunctionName=None,
    InputVar='data',
    OutputVar='rval',
    ErrorVar='errs',
    Prefix='',
  ):
    Opt = self.PythonOpt(
      FunctionName=FunctionName,
      InputVar=InputVar,
      OutputVar=OutputVar,
      ErrorVar=ErrorVar,
    )

    Lines = []
    VarDepth = 0

    if Opt.FunctionName:
      Lines.append(Prefix + f'def {Opt.FunctionName}({Opt.InputVar}):')
      Prefix += '  '
      Lines.append(Prefix + f'import collections, re')
      Lines.append(Prefix + f'from Granite import aadict, Undefined')

    Lines.append(Prefix + f'{Opt.ErrorVar} = []')

    Lines.append(Prefix + f'vi{VarDepth} = {Opt.InputVar}')
    Lines.append(Prefix + f'vo{VarDepth} = Undefined')
    self.Python_(self.StructPath, self.Struct, VarDepth, Prefix, Lines, Opt)
    Lines.append(Prefix + f'if {Opt.ErrorVar}:')
    Lines.append(Prefix + f'  raise Exception(str({Opt.ErrorVar}))')

    Lines.append(Prefix + f'{Opt.OutputVar} = vo{VarDepth}')

    if Opt.FunctionName:
      Lines.append(Prefix + f'return {Opt.OutputVar}')
      Prefix = Prefix[:-2]

    Lines.append(Prefix)

    return Lines

  def Struct_(self, StructPath, StructIn):
    if isinstance(StructIn, str):
      if StructIn.endswith('?'):
        StructIn = {'+Type': StructIn.removesuffix('?'), '+None': True}
      else:
        StructIn = {'+Type': StructIn}

    StructIn = dict(StructIn)  #copy it so we can pop keys off of it so we know if any are remaining (error)
    Struct = {}

    try:
      Struct['+Source'] = StructIn.pop('+Source', None)
      Struct['+Type'] = StructIn.pop('+Type')
      if Struct['+Type'].endswith('?'):
        Struct['+Type'] = Struct['+Type'].removesuffix('?')
        Struct['+None'] = True
        if '+None' in StructIn:
          raise TypeError(f'`+Type` ended with `?` yet `+None` was specified anyway at `{"/".join(StructPath)}`')
      else:
        Struct['+None'] = bool(StructIn.pop('+None', False))

      Struct['+Label'] = StructIn.pop('+Label', StructPath[-1])
      Struct['+Help'] = StructIn.pop('+Help', None)
    except KeyError as e:
      raise TypeError(f'KeyError at `{"/".join(StructPath)}`: {e}') from None

    if hasattr(self, fn:=f'Struct_{Struct["+Type"]}'):
      getattr(self, fn)(StructPath, StructIn, Struct)
    else:
      raise TypeError(f'Unrecognized type `{Struct["+Type"]}` at `{"/".join(StructPath)}`')

    Struct['+Default'] = copy.deepcopy(StructIn.pop('+Default', None))  # CRITICAL to clone this deeply so that we don't get shared values used as defaults

    if extra := set(StructIn) - set(Struct):
      raise TypeError(f'Unrecognized attributes for type `{Struct["+Type"]}` at `{"/".join(StructPath)}`: {", ".join(extra)}')

    return Struct

  def Python_(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt, *, KeyVar=None, ValueVar=None):
    if not hasattr(self, fn:=f'Python_{Struct["+Type"]}'):
      raise TypeError(f'No method `{fn}` to generate Python code for type `{Struct["+Type"]}` at `{"/".join(StructPath)}`')

    Lines.append(Prefix +     f'# START {"/".join(StructPath)}')

    Lines.append(Prefix +     f'try:')

    if Struct['+Default'] is not None:
      Lines.append(Prefix +   f'  if vi{VarDepth} is None:')
      Lines.append(Prefix +   f'    vi{VarDepth} = ' + repr(Struct['+Default']))
      getattr(self, fn)(StructPath, Struct, VarDepth, Prefix + '  ', Lines, Opt)
    else:  
      Lines.append(Prefix +   f'  if vi{VarDepth} is None:')
      if Struct['+None']:
        Lines.append(Prefix + f'    vo{VarDepth} = None')
      else:
        Lines.append(Prefix + f'    raise ValueError("Value must not be None")')
      Lines.append(Prefix +   f'  else:')
      getattr(self, fn)(StructPath, Struct, VarDepth, Prefix + '    ', Lines, Opt)

    Lines.append(Prefix +     f'except (ValueError, TypeError) as e:')
    Lines.append(Prefix +     f'  {Opt.ErrorVar}.append(({repr("/".join(StructPath))}, str(e), {KeyVar}, {ValueVar}))')


    Lines.append(Prefix + f'# END {"/".join(StructPath)}')

  def Struct_Type(self, StructPath, StructIn, Struct):
    pass

  def Python_Type(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'vo{VarDepth} = vi{VarDepth}')

  def Struct_Boolean(self, StructPath, StructIn, Struct):
    pass

  def Python_Boolean(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'vo{VarDepth} = bool(vi{VarDepth})')

  def Struct_Integer(self, StructPath, StructIn, Struct):
    Struct['+MaxValue'] = INTN(StructIn.pop('+MaxValue', None))
    Struct['+MinValue'] = INTN(StructIn.pop('+MaxValue', None))

  def Python_Integer(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'vo{VarDepth} = int(vi{VarDepth})')

  def Struct_Decimal(self, StructPath, StructIn, Struct):
    Struct['+MaxValue'] = DECIMALN(StructIn.pop('+MaxValue', None))
    Struct['+MinValue'] = DECIMALN(StructIn.pop('+MaxValue', None))

  def Python_Decimal(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'vo{VarDepth} = Decimal(vi{VarDepth})')

  def Struct_Float(self, StructPath, StructIn, Struct):
    Struct['+MaxValue'] = DECIMALN(StructIn.pop('+MaxValue', None))
    Struct['+MinValue'] = DECIMALN(StructIn.pop('+MaxValue', None))

  def Python_Float(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'vo{VarDepth} = float(vi{VarDepth})')

  def Struct_Enum(self, StructPath, StructIn, Struct):
    Struct['+Values'] = StructIn.pop('+Values')

  def Python_Enum(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'vo{VarDepth} = vi{VarDepth}')
    Lines.append(Prefix + f'if vo{VarDepth} not in {repr(Struct["+Values"])}:')
    Lines.append(Prefix + f'  raise ValueError("Value must be one of {repr(Struct["+Values"])}")')

  def Struct_String(self, StructPath, StructIn, Struct):
    Struct['+MaxLength'] = INTN(StructIn.pop('+MaxLength', None))
    Struct['+MinLength'] = INTN(StructIn.pop('+MinLength', None))
    Struct['+Strip'] = BOOLN(StructIn.pop('+Strip', True))
    Struct['+Regex'] = STRN(StructIn.pop('+Regex', None))


  def Python_String(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'vo{VarDepth} = str(vi{VarDepth})')

    if Struct['+Strip'] is not None:
      Lines.append(Prefix + f'vo{VarDepth} = vo{VarDepth}.strip()')

    if Struct['+MinLength'] is not None:
      Lines.append(Prefix + f'if len(vo{VarDepth}) > {repr(Struct["+MinLength"])}:')
      Lines.append(Prefix + f'  raise ValueError("Input too short")')

    if Struct['+MaxLength'] is not None:
      Lines.append(Prefix + f'if len(vo{VarDepth}) > {repr(Struct["+MaxLength"])}:')
      Lines.append(Prefix + f'  raise ValueError("Input too long")')

    if Struct['+Regex'] is not None:
      Lines.append(Prefix + f'if not re.match({repr(Struct["+Regex"])}, vo{VarDepth}):')
      Lines.append(Prefix + f'  raise ValueError("Does not match regex: {Struct["+Regex"]}")')

  def Struct_Object(self, StructPath, StructIn, Struct):
    Struct['+Extra'] = BOOLN(StructIn.pop('+Extra', False))
    for k in tuple(StructIn):
      if k.startswith('+'):
        continue
      Struct[k] = self.Struct_(StructPath + (k,), StructIn.pop(k))

  def Python_Object(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'if isinstance(vi{VarDepth}, collections.abc.Mapping):')
    Lines.append(Prefix + f'  vo{VarDepth} = aadict()')

    for fieldname, fieldstruct in Struct.items():
      if fieldname.startswith('+'):
        continue

      Lines.append(Prefix + f'  vi{VarDepth+1} = vi{VarDepth}.get({repr(fieldname)})')
      Lines.append(Prefix + f'  vo{VarDepth+1} = Undefined')
      self.Python_(StructPath + (fieldname,), fieldstruct, VarDepth+1, Prefix + '  ', Lines, Opt, KeyVar=f'vi{VarDepth+1}')
      Lines.append(Prefix + f'  vo{VarDepth}[{repr(fieldname)}] = vo{VarDepth+1}')
      Lines.append(Prefix)

    if Struct['+Extra']:
      Lines.append(Prefix + f'  for k, v in vi{VarDepth}.items():')
      Lines.append(Prefix + f'    if k not in ' + repr(tuple(Struct)) + ':')
      Lines.append(Prefix + f'      vo{VarDepth}[k] = v')

    Lines.append(Prefix + f'else:')
    Lines.append(Prefix + f'  raise ValueError(f"Invalid type: {{vi{VarDepth}}}")')


  def Struct_Map(self, StructPath, StructIn, Struct):
    if '+KeyType' in StructIn:
      Struct['+KeyType'] = self.Struct_(StructPath + ('+KeyType',), StructIn.pop('+KeyType'))
    else:
      raise TypeError(f'Missing `+KeyType` for type `{Struct["+Type"]}` at `{"/".join(StructPath)}`')

    if '+ValueType' in StructIn:
      Struct['+ValueType'] = self.Struct_(StructPath + ('+ValueType',), StructIn.pop('+ValueType'))
    else:
      raise TypeError(f'Missing `+ValueType` for type `{Struct["+Type"]}` at `{"/".join(StructPath)}`')

  def Python_Map(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):

    Lines.append(Prefix + f'if isinstance(vi{VarDepth}, collections.abc.Mapping):')
    Lines.append(Prefix + f'  vo{VarDepth} = {{}}')
    Lines.append(Prefix + f'  for vi{VarDepth+1}k, vi{VarDepth+1}v in vi{VarDepth}.items():')

    Lines.append(Prefix + f'    # Process Key')
    Lines.append(Prefix + f'    vi{VarDepth+1} = vi{VarDepth+1}k')
    Lines.append(Prefix + f'    vo{VarDepth+1} = Undefined')
    self.Python_(StructPath + ('+KeyType',), Struct['+KeyType'], VarDepth+1, Prefix + '    ', Lines, Opt, KeyVar=f'vi{VarDepth+1}k')
    Lines.append(Prefix + f'    vo{VarDepth+1}k = vo{VarDepth+1}')

    Lines.append(Prefix)

    Lines.append(Prefix + f'    # Process Value')
    Lines.append(Prefix + f'    vi{VarDepth+1} = vi{VarDepth+1}v')
    Lines.append(Prefix + f'    vo{VarDepth+1} = Undefined')
    self.Python_(StructPath + ('+ValueType',), Struct['+ValueType'], VarDepth+1, Prefix + '    ', Lines, Opt, KeyVar=f'vi{VarDepth+1}k', ValueVar=f'vi{VarDepth+1}v')
    Lines.append(Prefix + f'    vo{VarDepth+1}v = vo{VarDepth+1}')

    Lines.append(Prefix)

    Lines.append(Prefix + f'    vo{VarDepth}[vo{VarDepth+1}k] = vo{VarDepth+1}v')

    Lines.append(Prefix + f'else:')
    Lines.append(Prefix + f'  raise ValueError(f"Must be Mapping: {{vi{VarDepth}}}")')

    Lines.append(Prefix)


  def Struct_Set(self, StructPath, StructIn, Struct):
    if '+ValueType' in StructIn:
      Struct['+ValueType'] = self.Struct_(StructPath + ('+ValueType',), StructIn.pop('+ValueType'))
    else:
      raise TypeError(f'Missing `+ValueType` for type `{Struct["+Type"]}` at `{"/".join(StructPath)}`')

  def Python_Set(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix + f'if isinstance(vi{VarDepth}, collections.abc.Iterable):')
    Lines.append(Prefix + f'  vo{VarDepth} = set()')
    Lines.append(Prefix + f'  for vi{VarDepth+1} in vi{VarDepth}:')

    Lines.append(Prefix + f'    vo{VarDepth+1} = Undefined')
    self.Python_(StructPath + ('+ValueType',), Struct['+ValueType'], VarDepth+1, Prefix + '    ', Lines, Opt, ValueVar=f'vi{VarDepth+1}')
    Lines.append(Prefix + f'    vo{VarDepth}.add(vo{VarDepth+1})')

    Lines.append(Prefix + f'else:')
    Lines.append(Prefix + f'  raise ValueError(f"Must be Iterable: {{vi{VarDepth}}}")')

    Lines.append(Prefix)

  def Struct_List(self, StructPath, StructIn, Struct):
    Struct['+Length'] = INTN(StructIn.pop('+Length', None))
    Struct['+MaxLength'] = INTN(StructIn.pop('+MaxLength', None))
    Struct['+MinLength'] = INTN(StructIn.pop('+MinLength', None))
    
    if '+ValueType' in StructIn:
      Struct['+ValueType'] = self.Struct_(StructPath + ('+ValueType',), StructIn.pop('+ValueType'))
    else:
      raise TypeError(f'Missing `+ValueType` for type `{Struct["+Type"]}` at `{"/".join(StructPath)}`')

  def Python_List(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    Lines.append(Prefix   + f'if isinstance(vi{VarDepth}, str):')
    Lines.append(Prefix   + f'  raise TypeError(f"Must be Iterable but not a string: {{vi{VarDepth}}}")')
    Lines.append(Prefix   + f'elif isinstance(vi{VarDepth}, collections.abc.Iterable):')
    Lines.append(Prefix   + f'  vo{VarDepth} = []')
    Lines.append(Prefix   + f'  for vi{VarDepth+1} in vi{VarDepth}:')

    Lines.append(Prefix   + f'    vo{VarDepth+1} = Undefined')
    self.Python_(StructPath + ('+ValueType',), Struct['+ValueType'], VarDepth+1, Prefix + '    ', Lines, Opt, ValueVar=f'vi{VarDepth+1}')
    Lines.append(Prefix   + f'    vo{VarDepth}.append(vo{VarDepth+1})')
    Lines.append(Prefix   + f'  pass#for')
    
    if Struct['+Length'] is not None:
      Lines.append(Prefix + f'  # +Length')
      Lines.append(Prefix + f'  if len(vo{VarDepth}) != {repr(Struct["+Length"])}:')
      Lines.append(Prefix + f'    raise ValueError(f"List must contain exactly {Struct["+Length"]} items, but contains {{len(vo{VarDepth})}} items.")')
    
    if Struct['+MaxLength'] is not None:
      Lines.append(Prefix + f'  # +MaxLength')
      Lines.append(Prefix + f'  if len(vo{VarDepth}) > {repr(Struct["+MaxLength"])}:')
      Lines.append(Prefix + f'    raise ValueError("List must contain at most {Struct["+MaxLength"]} items.")')
    
    if Struct['+MinLength'] is not None:
      Lines.append(Prefix + f'  # +MinLength')
      Lines.append(Prefix + f'  if len(vo{VarDepth}) < {repr(Struct["+MinLength"])}:')
      Lines.append(Prefix + f'    raise ValueError("List must contain at least {Struct["+MinLength"]} items.")')


    Lines.append(Prefix + f'else:')
    Lines.append(Prefix + f'  raise TypeError(f"Must be Iterable: {{vi{VarDepth}}}")')

    Lines.append(Prefix)


  def Struct_Email(self, StructPath, StructIn, Struct):
    self.Struct_String(StructPath, StructIn, Struct)

  def Python_Email(self, StructPath, Struct, VarDepth, Prefix, Lines, Opt):
    self.Python_String(StructPath, Struct, VarDepth, Prefix, Lines, Opt)






########################################################################################################################

if postgresql:

  #
  IS_IDENTIFIER = re.compile(r'^[a-zA-Z0-9_][a-zA-Z0-9_()\[\]@|-]*$').match
  IS_DOLLAR_PARAM = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$').match
  IS_SAFESTRING = re.compile(r'^[a-zA-Z0-9_ .:;,+=-]*$').match
  NAMED_PARAM_SPLITTER = re.compile(r'\$([a-zA-Z][a-zA-Z0-9_]*)').split
  FIELD_VALUE_SPLITTER = re.compile(r'\[(Field|Value|Field=Value)\]').split
  DYNAMIC_SCHEMA_SPLITTER = re.compile(r'"([a-zA-Z][a-zA-Z0-9_]+)\[\]"\.').split

  # works on `Schema[...]` or `Schema[...].Table` (e.g. FOO[BAR] or FOO[BAR].BAZ)
  # Designed to extract the ... above
  EXTRACT_SCHEMA_KEY = re.compile(r'^VM4\[([a-zA-Z0-9]+)\](\.[a-zA-Z][a-zA-Z0-9_]+)?$').match  




  # ==============================================================================
  class NotOneFound(DataNotFoundError):
    pass


  class TransactionInProgressError(Exception):
    pass


  # Alias for this file only
  NotOneFoundType = NotOneFound



  # ==============================================================================
  class PreparedStatement(postgresql.driver.pq3.PreparedStatement):

    # ----------------------------------------------------------------------------
    def Execute(self, *Params, NotOneFound=None):
      '''
      The driver will either return a list of tuples for a SELECT or a 2-tuple
      for other kinds of operations.
      '''
      r = self(*Params)

      if isinstance(r, list):
        rows_affected = len(r)
      elif isinstance(r, tuple):
        rows_affected = r[1]
      else:
        raise TypeError('Unknown return type from Execute: `{}`'.format(type(r)))

      if rows_affected != 1:
        if NotOneFound is None:
          return None
        elif NotOneFound is NotOneFoundType:
          raise NotOneFoundType("Exactly 1 row expected, but {0} found.".format(rows_affected))
        elif NotOneFound is AuthorizationError:
          raise AuthorizationError("Not authorized to access this resource.")

      return r

    # ----------------------------------------------------------------------------
    def Value(self, *Params, NotOneFound=NotOneFoundType):
      r = self(*Params)
      if len(r) != 1:
        if NotOneFound is NotOneFoundType:
          raise NotOneFoundType("Exactly 1 row expected, but {0} found.".format(len(r)))
        elif NotOneFound is AuthorizationError:
          raise AuthorizationError("Not authorized to access this resource.")
        elif NotOneFound is None:
          return None
      return r[0][0]

    # ----------------------------------------------------------------------------
    def ValueList(self, *Params):
      return [r[0] for r in self(*Params)]

    # ----------------------------------------------------------------------------
    def ValueDict(self, *Params):
      return dict((r[0], r[1]) for r in self(*Params))

    # ----------------------------------------------------------------------------
    def ValueSet(self, *Params):
      return {r[0] for r in self(*Params)}

    # ----------------------------------------------------------------------------
    def Row(self, *Params, NotOneFound=NotOneFoundType):
      r = self(*Params)
      if len(r) != 1:
        if NotOneFound is NotOneFoundType:
          raise NotOneFoundType("Exactly 1 row expected, but {0} found.".format(len(r)))
        elif NotOneFound is AuthorizationError:
          raise AuthorizationError("Not authorized to access this resource.")
        elif NotOneFound is None:
          return None
      return aadict(r[0])

    # ----------------------------------------------------------------------------
    def RowList(self, *Params):
      return [aadict(r) for r in self(*Params)]

    # ----------------------------------------------------------------------------
    def RowDict(self, *Params):
      return dict((r[0], aadict(r)) for r in self(*Params))

    # ----------------------------------------------------------------------------
    def TRow(self, *Params, NotOneFound=NotOneFoundType):
      r = self(*Params)
      if len(r) != 1:
        if NotOneFound is NotOneFoundType:
          raise NotOneFoundType("Exactly 1 row expected, but {0} found.".format(len(r)))
        elif NotOneFound is AuthorizationError:
          raise AuthorizationError("Not authorized to access this resource.")
        elif NotOneFound is None:
          return None
      return tuple(r[0])

    # ----------------------------------------------------------------------------
    def TRowList(self, *Params):
      return [tuple(r) for r in self(*Params)]

    # ----------------------------------------------------------------------------
    def Bool(self, *Params, **kwargs):
      try:
        return bool(self.Value(*Params, **kwargs))
      except NotOneFoundType:
        return False


  # ==============================================================================
  class Connection(postgresql.driver.pq3.Connection):
    PreparedStatement = PreparedStatement
    NotOneFound = NotOneFoundType
    UniqueError = postgresql.exceptions.UniqueError
    Exceptions = postgresql.exceptions

    def ExtractSchemaKey(self, Schema):
      if match := EXTRACT_SCHEMA_KEY(Schema):
        return match.group(1)
      else:
        raise ValueError(f'Invalid Schema: {Schema}')

    # ----------------------------------------------------------------------------

    def Transaction(self, *args, **kwargs):
      return postgresql.driver.pq3.Connection.xact(self, *args, **kwargs)

    def NotNestedTransaction(self, *args, **kwargs):
      if self.pq.state != b'I':
        raise TransactionInProgressError('A transaction is already in progress and an unnested transaction cannot be started.')
      return self.Transaction(*args, **kwargs)

    def NestedTransaction(self, *args, **kwargs):
      if self.pq.state != b'T':
        raise TransactionInProgressError('A transaction is not in progress and a nested transaction cannot be started.')
      return self.Transaction(*args, **kwargs)

    # ----------------------------------------------------------------------------
    @classmethod
    def Assume(cls, conn):
      """
      Call this on an an instance of pq3.Connection, and it will change its
      class to this class (correctly).

      """
      if not isinstance(conn, postgresql.driver.pq3.Connection):
        raise TypeError("Connection must be an instances of {0}: {1}".format(
          str(postgresql.driver.pq3.Connection),
          str(conn)
        ))

      if conn.__class__ is cls:
        raise RuntimeError("This function has already been called on {0}.".format(
          str(conn)
        ))

      conn.__class__ = cls

      # this code is essentially an __init__ for the new class, but since
      # __init__ has already been called in the base class, let's not confuse
      # matters...
      self = conn
      self._PS_Cache = {}

    # ----------------------------------------------------------------------------
    # Debugging hackery functions
    # Call only once per application startup (e.g. in AppLoader) if you want it enabled
    # ONLY USE IN DEBUGGING MODE

    @classmethod
    def EnableSlowLog(cls, min_time):
      def callfun(self, *args, **kwargs):
        ts = time.time()
        try:
          return postgresql.driver.pq3.PreparedStatement.__call__(self, *args, **kwargs)
        finally:
          td = round(time.time() - ts, 6)
          sa = round(ts-App.EnterTime, 3)
          if not min_time or td > min_time:
            if sa < 60:
              App.LogTime('App.EnableSlowLog', Duration=td, SinceAppEnter=sa, Query=self.string, RequestID=App.RequestID)

      PreparedStatement.__call__ = callfun

    # ----------------------------------------------------------------------------
    def QuoteIdentifier(self, name):
      if not IS_IDENTIFIER(name):
        raise ValueError("Invalid identifier passed in argument 1: {0}".format(name))
      return '"' + name + '"'

    # ----------------------------------------------------------------------------
    def DollarParameter(self, name):
      if not IS_DOLLAR_PARAM(name):
        raise ValueError("Invalid identifier passed in argument 1: {0}".format(name))
      return '$' + name

    # ----------------------------------------------------------------------------
    def QuoteString(self, value):
      # TODO: audit this
      return "E" + repr(str(value)) + ""

    # ----------------------------------------------------------------------------
    def Literal(self, value):
      if isinstance(value, bool):
        if value is None:
          return 'NULL'
        elif value:
          return 'True'
        else:
          return 'False'

      elif isinstance(value, int):
        return str(value)

      elif isinstance(value, str):
        return self.QuoteString(value)

      else:
        raise TypeError('Cannot process value of type `{}`.'.format(type(value)))

    # ----------------------------------------------------------------------------
    # override anything that returns prepared statements from the base class
    def prepare(self, *args, **kwargs):
      ps = super().prepare(*args, **kwargs)
      ps.__class__ = self.PreparedStatement
      return ps

    # ----------------------------------------------------------------------------
    def statement_from_id(self, *args, **kwargs):
      ps = super().statement_from_id(*args, **kwargs)
      ps.__class__ = self.PreparedStatement
      return ps

    # ----------------------------------------------------------------------------
    # prepared statement cache (can be implemented later)
    def CachePrepare(self, SQL):
      if SQL not in self._PS_Cache:
        self._PS_Cache[SQL] = self.prepare(SQL)
      return self._PS_Cache[SQL]

    # ----------------------------------------------------------------------------
    def PrePrepare(self, sql_text, args, kwargs):
      """
      Takes a SQL statement, a variable number of positional args, and a variable
      number of keword args, and returns a 3-tuple of:

        (SQL string, [args data, ...], ARGS instance)

      return[0] is suitable for the SQL for a prepared statement
      return[1] is suitable for passing as args to execute a prepared statement
      return[2] is suitable for passing as **kwargs to downstream methods

      --
      All positional arguments must be tuples with len() of 1-3 OR ARGS() instances

      For ARGS:
        These will be will be used as arguments to the downstream functions

      For Tuples:
        Position 1 is the field name
        Position 2 is the field value
        Position 3 is the SQL

      It may be called in these ways, which result in:

       "Name"                        -> ("Name", None,  "$Name")
      ("Name",)                      -> ("Name", None,  "$Name")
      ("Name", value)                -> ("Name", value, "$Name")
      ("Name", SQL("sql"))           -> ("Name", None,  "sql")
      ("Name", value, "sql")         -> ("Name", value, "sql")


      The keyword arguments must be in the form:
        FieldName = FieldValue

      So what is the difference?

        - The positional arguments are used as the difinitive list of fields and
          values used to replace [Field], [Value], and [Field=Value] in the SQL.

        - Keyword arguments are simple key value pairs and should coorspond to
          a $fieldname parameter present in the SQL.

      The SQL passed to this function supports the following replacements:

        [Field]
        [Value]
        [Field=Value]

      They will be replaced with the fields passed as positional arguments.

        $varname
        $other_var_name

      Will be replaced with $1, $2, etc... with the correct value from the
      positional or keyword arguments returned in the correct order as return[1].
      """

      Args = ARGS()
      DataMap = {}
      FieldNames = []
      FieldValues = []

      # Special handling of keyword arguments
      if 'NotOneFound' in kwargs:
        Args['NotOneFound'] = kwargs['NotOneFound']
        del kwargs['NotOneFound']

      # Process each argument
      for T in args:

        if isinstance(T, str):
          T = (T,)
          L = 1
        elif isinstance(T, tuple):
          L = len(T)
          if L == 0:
            raise ValueError("Empty tuple passed as argument: {0}".format(T))
        elif isinstance(T, ARGS):
          Args.update(T)
          continue
        else:
          raise ValueError("Positional parmaters must be str or tuple, not: {0}".format(T))

        # Special handling of RecordLog()
        if T[0] == 'RecordLog()':
          DataMap['RecordLog__'] = JSON_Encode(T[1])
          FieldNames.append('"RecordLog()"')
          FieldValues.append('$RecordLog__')
          continue

        # All other fields
        if not IS_IDENTIFIER(T[0]):
          raise ValueError("Invalid field name passed in argument: {0}".format(T))

        FieldNames.append('"' + T[0] + '"')

        # One way or another, we will get th FieldNames, DataMap, and FieldValues filled in
        if L == 1:
          DataMap[T[0]] = None
          FieldValues.append("$" + T[0])
        elif L == 2 and isinstance(T[1], SQL):
          DataMap[T[0]] = None
          FieldValues.append(str(T[1]))
        elif L == 2:
          DataMap[T[0]] = T[1]
          FieldValues.append("$" + T[0])
        elif L == 3 and isinstance(T[1], SQL):
          raise ValueError('A 3-tuple argument must NOT have an SQL instance as the 2nd value')
        elif L == 3:
          DataMap[T[0]] = T[1]
          FieldValues.append(T[2])
        else:
          raise ValueError("Invalid tuple passed as argument: {0}".format(T))

      for T in kwargs.items():
        if not IS_IDENTIFIER(T[0]):
          raise ValueError("Invalid field name passed in keyword argument: {0}".format(T))

        if T[0] in DataMap:
          raise KeyError("Keyword argument '{0}' encountered which was already defined by a positional argument.".format(T[0]))

        DataMap[T[0]] = T[1];

      # Handle replacment of [Field], [Value], and [Field=Value]
      split_sql = FIELD_VALUE_SPLITTER(sql_text)
      for i in range(1, len(split_sql), 2):

        if split_sql[i] == 'Field':
          split_sql[i] = str.join(", ", FieldNames)

        elif split_sql[i] == 'Value':
          split_sql[i] = str.join(", ", FieldValues)

        elif split_sql[i] == 'Field=Value':
          split_sql[i] = str.join(", ", (f + "=" + v for f, v in zip(FieldNames, FieldValues)))

      sql_text = str.join("", split_sql)

      # Handle conversion of $field and $name to $1 and $2
      split_sql = NAMED_PARAM_SPLITTER(sql_text)

      pos = 0
      ParamList = []
      for i in range(1, len(split_sql), 2):
        pos += 1
        try:
          dv = DataMap[split_sql[i]]

          if dv is NULL:
            dv = None

          ParamList.append(dv)
          split_sql[i] = "$" + str(pos)
        except KeyError:
          raise KeyError("Field name '{0}' not found in positional or keyword arguments, despite being referenced in this SQL: {1}".format(split_sql[i], sql_text))

      sql_text = ''.join(split_sql) 

      # Look for Schema that has '''"SchemaName[]".''' 
      split_sql = DYNAMIC_SCHEMA_SPLITTER(sql_text)
      
      for i in range(1, len(split_sql), 2):
        try:
          schemakey = App.DB_SchemaKeyMap[split_sql[i]]
          split_sql[i] = '"' + split_sql[i] + '[' + schemakey + ']".'
        except KeyError:
          raise KeyError(f'Schema Key "{split_sql[i]}" not found in App.DB_SchemaKeyMap, despite being referenced in this SQL: {SQL}')

      sql_text = ''.join(split_sql) 
      
      return (sql_text, tuple(ParamList), Args)



    # ----------------------------------------------------------------------------
    # Get a single value
    def Execute(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).Execute(*Params, **Args)

    # ----------------------------------------------------------------------------
    # Get a block of SQL statements
    def ExecuteRaw(self, sql_text):
      return self.execute(sql_text)

    # ----------------------------------------------------------------------------
    # Get a single value
    def Value(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).Value(*Params, **Args)

    # ----------------------------------------------------------------------------
    # Get a list of values (first column)
    def ValueList(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).ValueList(*Params, **Args)

    # ----------------------------------------------------------------------------
    # Get a dict of value => value (first column => second column)
    def ValueDict(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).ValueDict(*Params, **Args)

    # ----------------------------------------------------------------------------
    # Get a set of values (first column)
    def ValueSet(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).ValueSet(*Params, **Args)

    # ----------------------------------------------------------------------------
    def Row(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).Row(*Params, **Args)

    # ----------------------------------------------------------------------------
    def RowList(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).RowList(*Params, **Args)

    # ----------------------------------------------------------------------------
    # get a dict of rows, keyed by the first column
    def RowDict(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).RowDict(*Params, **Args)

    # ----------------------------------------------------------------------------
    def TRow(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).TRow(*Params, **Args)

    # ----------------------------------------------------------------------------
    def TRowList(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).TRowList(*Params, **Args)

    # ----------------------------------------------------------------------------
    def Bool(self, sql_text, *args, **kwargs):
      sql_text, Params, Args = self.PrePrepare(sql_text, args, kwargs)
      return self.CachePrepare(sql_text).Bool(*Params, **Args)

    # ----------------------------------------------------------------------------
    def Delete(self, Schema, Table, **kwargs):
      '''
      Pass a schema and table and simple set of conditions (fields) that must be true
      '''

      if len(kwargs) == 0:
        raise TypeError('At least one keyword argument is required')

      if not IS_IDENTIFIER(Schema):
        raise ValueError("Invalid schema name passed in argument 1: {0}".format(Schema))

      if not IS_IDENTIFIER(Table):
        raise ValueError("Invalid table name passed in argument 2: {0}".format(Table))

      sql = 'DELETE FROM \n  "' + Schema + '"."' + Table + '"\nWHERE True\n'

      for field, value in kwargs.items():
        if not IS_DOLLAR_PARAM(field):
          raise ValueError("Invalid field name passed as keyword argument: {0}={1}".format(field, value))

        if value is None:
          sql += '  AND "' + field + '" IS NULL\n'
        else:
          sql += '  AND "' + field + '" = $' + field + '\n'

      return self.Execute(sql, **kwargs)

    # ----------------------------------------------------------------------------
    def Exists(self, Schema, Table, **kwargs):
      '''
      Pass a schema and table and simple set of conditions (fields) that must be true.
      Will return if a record like that exists
      '''

      if not IS_IDENTIFIER(Schema):
        raise ValueError("Invalid schema name passed in argument 1: {0}".format(Schema))

      if not IS_IDENTIFIER(Table):
        raise ValueError("Invalid table name passed in argument 2: {0}".format(Table))

      sql = 'SELECT EXISTS (\n  SELECT 1\n  FROM "' + Schema + '"."' + Table + '"\n  WHERE True\n'

      for field, value in kwargs.items():
        if not IS_DOLLAR_PARAM(field):
          raise ValueError("Invalid field name passed as keyword argument: {0}={1}".format(field, value))

        if value is None:
          sql += '  AND "' + field + '" IS NULL\n'
        else:
          sql += '  AND "' + field + '" = $' + field + '\n'

      sql += ')'

      return self.Bool(sql, **kwargs)

    # ----------------------------------------------------------------------------
    def Select(self, Schema, Table, *fields, **kwargs):
      '''
      Pass a schema and table and a listsimple set of conditions (fields) that must be true.
      Will return if a record like that exists
      '''

      if not IS_IDENTIFIER(Schema):
        raise ValueError("Invalid schema name passed in argument 1: {0}".format(Schema))

      if not IS_IDENTIFIER(Table):
        raise ValueError("Invalid table name passed in argument 2: {0}".format(Table))

      select_list = []
      for field in fields:
        if isinstance(field, SQL):
          select_list.append(field)

        elif IS_IDENTIFIER(field):
          select_list.append('"' + field + '"')

        else:
          raise ValueError("Invalid field name passed as arg: {0}".format(repr(field)))

      if len(select_list) == 0:
        raise TypeError('Must pass at least one field to select')

      sql = 'SELECT \n  ' + str.join(', ', select_list) + '\nFROM "' + Schema + '"."' + Table + '"\n  WHERE True\n'

      for field, value in kwargs.items():
        if field in ('NotOneFound',):
          continue

        if not IS_DOLLAR_PARAM(field):
          raise ValueError("Invalid field name passed as keyword argument: {0}={1}".format(field, value))

        if value is None:
          sql += '  AND "' + field + '" IS NULL\n'
        else:
          sql += '  AND "' + field + '" = $' + field + '\n'

      if len(select_list) == 1:
        return self.Value(sql, **kwargs)
      else:
        return self.Row(sql, **kwargs)

    # ----------------------------------------------------------------------------
    def SelectValue(self, Schema, Table, field, **kwargs):
      '''
      Pass a schema and table and a listsimple set of conditions (fields) that must be true.
      Will return if a record like that exists
      '''

      if not IS_IDENTIFIER(Schema):
        raise ValueError("Invalid schema name passed in argument 1: {0}".format(Schema))

      if not IS_IDENTIFIER(Table):
        raise ValueError("Invalid table name passed in argument 2: {0}".format(Table))

      if isinstance(field, SQL):
        select_sql = field
      elif IS_IDENTIFIER(field):
        select_sql = '"' + field + '"'
      else:
        raise ValueError("Invalid field name passed as arg: {0}".format(repr(field)))

      sql = 'SELECT \n  ' + select_sql + '\nFROM "' + Schema + '"."' + Table + '"\n  WHERE True\n'

      for field, value in kwargs.items():
        if field in ('NotOneFound',):
          continue

        if not IS_DOLLAR_PARAM(field):
          raise ValueError("Invalid field name passed as keyword argument: {0}={1}".format(field, value))

        if value is None:
          sql += '  AND "' + field + '" IS NULL\n'
        else:
          sql += '  AND "' + field + '" = $' + field + '\n'

      return self.Value(sql, **kwargs)

    # ----------------------------------------------------------------------------
    def SelectRow(self, Schema, Table, *fields, **kwargs):
      '''
      Pass a schema and table and a listsimple set of conditions (fields) that must be true.
      Will return if a record like that exists
      '''

      if not IS_IDENTIFIER(Schema):
        raise ValueError("Invalid schema name passed in argument 1: {0}".format(Schema))

      if not IS_IDENTIFIER(Table):
        raise ValueError("Invalid table name passed in argument 2: {0}".format(Table))

      select_list = []
      if fields == ('*',):
        select_list.append('*')
      else:
        for field in fields:
          if isinstance(field, SQL):
            select_list.append(field)

          elif IS_IDENTIFIER(field):
            select_list.append('"' + field + '"')

          else:
            raise ValueError("Invalid field name passed as arg: {0}".format(repr(field)))

      if len(select_list) == 0:
        raise TypeError('Must pass at least one field to select')

      sql = 'SELECT \n  ' + str.join(', ', select_list) + '\nFROM "' + Schema + '"."' + Table + '"\n  WHERE True\n'

      for field, value in kwargs.items():
        if field in ('NotOneFound',):
          continue

        if not IS_DOLLAR_PARAM(field):
          raise ValueError("Invalid field name passed as keyword argument: {0}={1}".format(field, value))

        if value is None:
          sql += '  AND "' + field + '" IS NULL\n'
        else:
          sql += '  AND "' + field + '" = $' + field + '\n'

      return self.Row(sql, **kwargs)

    # ----------------------------------------------------------------------------
    def Insert(self, Schema, Table, *args, Returning=None):
      '''
      Pass a schema and table and a set of field spec tuples
      Returning can be either:
        a. None (nothing is returned)
        b. String or SQL (a value is returned)
        c. Sequence of Strings or SQL (row is returned)

      NOTE: always check for SQL before str because SQL is a subclass of str.
      '''

      if not IS_IDENTIFIER(Schema):
        raise ValueError("Invalid schema name passed in argument 1: {0}".format(Schema))

      if not IS_IDENTIFIER(Table):
        raise ValueError("Invalid table name passed in argument 2: {0}".format(Table))

      sql = 'INSERT INTO \n  "' + Schema + '"."' + Table + '"\n  ([Field]) \nVALUES\n  ([Value])\n'

      if Returning is None:
        return self.Execute(sql, *args)

      elif isinstance(Returning, SQL):
        sql += 'RETURNING ' + Returning + '\n'
        return self.Value(sql, *args)

      elif isinstance(Returning, str):
        if not IS_IDENTIFIER(Returning):
          raise ValueError("Invalid Returning value passed: {0}".format(Returning))
        sql += 'RETURNING "' + Returning + '"\n'
        return self.Value(sql, *args)

      elif isinstance(Returning, collections.abc.Sequence):
        fields = []
        for i, val in enumerate(Returning):
          if isinstance(val, SQL):
            fields.append(val)
          elif isinstance(val, str):
            if not IS_IDENTIFIER(val):
              raise ValueError("Invalid Returning value passed at sequence position {0}: {1}".format(i, val))
            fields.append('"' + val + '"')
          else:
            raise TypeError("Invalid Returning value type passed at sequence position {0}: {1}".format(i, val))

        sql += 'RETURNING ' + str.join(', ', fields) + '\n'
        return self.Row(sql, *args)

      else:
        raise TypeError("Invalid Returning value type passed: {0}".format(Returning))

    # ----------------------------------------------------------------------------
    def Update(self, Schema, Table, *args, **kwargs):
      '''
      keyword arguments are WHERE
      regular arguments are fieldspec tuples

      Returning is a special keyword argument that can be either:
        a. None (nothing is returned)
        b. String or SQL (a value is returned)
        c. Sequence of Strings or SQL (row is returned)

      '''

      # Process special keyword argument
      if 'Returning' in kwargs:
        Returning = kwargs['Returning']
        del kwargs['Returning']
      else:
        Returning = None

      # Validation
      if len(kwargs) == 0:
        raise TypeError('At least one keyword argument is required that is a WHERE clause field')

      if not IS_IDENTIFIER(Schema):
        raise ValueError("Invalid schema name passed in argument 1: {0}".format(Schema))

      if not IS_IDENTIFIER(Table):
        raise ValueError("Invalid table name passed in argument 2: {0}".format(Table))

      # Build SQL
      sql = 'UPDATE \n  "' + Schema + '"."' + Table + '"\nSET\n  [Field=Value]\nWHERE True\n'

      for field, value in kwargs.items():
        if not IS_DOLLAR_PARAM(field):
          raise ValueError("Invalid field name passed as keyword argument: {0}={1}".format(field, value))

        if value is None:
          sql += '  AND "' + field + '" IS NULL\n'
        else:
          sql += '  AND "' + field + '" = $' + field + '\n'

      # Process Returning value
      if Returning is None:
        return self.Execute(sql, *args, **kwargs)

      elif Returning == '*':
        sql += 'RETURNING *\n'
        return self.Row(sql, *args, **kwargs)

      elif isinstance(Returning, SQL):
        sql += 'RETURNING ' + Returning + '\n'
        return self.Value(sql, *args, **kwargs)

      elif isinstance(Returning, str):
        if not IS_IDENTIFIER(Returning):
          raise ValueError("Invalid Returning value passed: {0}".format(Returning))
        sql += 'RETURNING "' + Returning + '"\n'
        return self.Value(sql, *args, **kwargs)

      elif isinstance(Returning, collections.abc.Sequence):
        fields = []
        for i, val in enumerate(Returning):
          if isinstance(val, SQL):
            fields.append(val)
          elif isinstance(val, str):
            if not IS_IDENTIFIER(val):
              raise ValueError("Invalid Returning value passed at sequence position {0}: {1}".format(i, val))
            fields.append('"' + val + '"')
          else:
            raise TypeError("Invalid Returning value type passed at sequence position {0}: {1}".format(i, val))

        sql += 'RETURNING ' + str.join(', ', fields) + '\n'
        return self.Row(sql, *args, **kwargs)

      else:
        raise TypeError("Invalid Returning value type passed: {0}".format(Returning))


  # ===================================================================================================================
  def OpenPostgres(*, onOpen=None, Host='localhost', Database, Role, Password, Port=5432) -> Connection:
    """A passthru to postgresql.open, which will convert the connection class"""
    conn = postgresql.open(host=Host, database=Database, user=Role, password=Password, port=Port, connect_timeout=5)
    Connection.Assume(conn)
    if onOpen:
      onOpen(conn)
    return conn

pass#if postgresql
########################################################################################################################


if redis:
  class Redis(redis.Redis):
    '''
    AppStruct wrapper around the `redis` library at:

      http://github.com/andymccurdy/redis-py v2.6.2

      Should compatible with future versions so long as
      the redis library mainains backward compatibility with the API

    --
    This class provides strongly typed wrapper functions around the basic
    redis operations on keys.

    In your app, you may use `set_bool` and `get_bool` to act as if redis
    is actually storing a Bool, but in fact, it is storing an int (0 or 1)

    '''

    # Key operations

    def keys_str(self, key):
      return [v.decode('utf-8') for v in self.keys(key)]

    # String operations

    def set_bool(self, key, value):
      return self.set(key, str(int(bool(value))))

    def get_bool(self, key):
      value = self.get(key)
      return None if value is None else bool(int(value))

    def set_int(self, key, value):
      return self.set(key, str(int(value)))

    def get_int(self, key):
      value = self.get(key)
      return None if value is None else int(value)

    def set_str(self, key, value):
      return self.set(key, str(value).encode('utf-8'))

    def get_str(self, key):
      value = self.get(key)
      return None if value is None else value.decode('utf-8')

    def mget_str(self, *keys):
      values = self.mget(*keys)
      return [None if value is None else value.decode('utf-8') for value in values]

    def set_json(self, key, value):
      return self.set(key, JSON_Encode(value).encode('utf-8'))

    def get_json(self, key):
      value = self.get(key)
      return None if value is None else JSON_Decode(value.decode('utf-8'))

    def mget_json(self, *keys):
      values = self.mget(*keys)
      return [None if value is None else JSON_Decode(value.decode('utf-8')) for value in values]

    def append_str(self, key, value):
      return self.append(key, str(value).encode('utf-8'))

    # Hash operations

    def hkeys_str(self, key):
      return [v.decode('utf-8') for v in self.hkeys(key)]

    def hvals_json(self, key):
      return [JSON_Decode(v.decode('utf-8')) for v in self.hvals(key)]

    def hset_bool(self, key, field, value):
      return self.hset(key, field, str(int(bool(value))))

    def hget_bool(self, key, field):
      value = self.hget(key, field)
      return None if value is None else bool(int(value))

    def hset_int(self, key, field, value):
      return self.hset(key, field, str(int(value)))

    def hget_int(self, key, field):
      value = self.hget(key, field)
      return None if value is None else int(value)

    def hset_str(self, key, field, value):
      return self.hset(key, field, str(value).encode('utf-8'))

    def hget_str(self, key, field):
      value = self.hget(key, field)
      return None if value is None else value.decode('utf-8')

    def hset_json(self, key, field, value):
      return self.hset(key, field, JSON_Encode(value).encode('utf-8'))

    def hget_json(self, key, field):
      value = self.hget(key, field)
      return None if value is None else JSON_Decode(value.decode('utf-8'))

    # List operations

    def lindex_bool(self, key, index):
      value = self.lpop(key, index)
      return None if value is None else bool(int(value))

    def lindex_int(self, key, index):
      value = self.lpop(key, index)
      return None if value is None else int(value)

    def lindex_str(self, key, index):
      value = self.lindex(key, index)
      return None if value is None else value.decode('utf-8')

    def lindex_json(self, key, index):
      value = self.lindex(key, index)
      return None if value is None else JSON_Decode(value.decode('utf-8'))

    def lpop_bool(self, key):
      value = self.lpop(key)
      return None if value is None else bool(int(value))

    def lpop_int(self, key):
      value = self.lpop(key)
      return None if value is None else int(value)

    def lpop_str(self, key):
      value = self.lpop(key)
      return None if value is None else value.decode('utf-8')

    def lpop_json(self, key):
      value = self.lpop(key)
      return None if value is None else JSON_Decode(value.decode('utf-8'))

    def lpush_bool(self, key, *args):
      return self.lpush(key, *[int(bool(arg)) for arg in args])

    def lpush_int(self, key, *args):
      return self.lpush(key, *[int(arg) for arg in args])

    def lpush_str(self, key, *args):
      return self.lpush(key, *[str(arg).encode('utf-8') for arg in args])

    def lpush_json(self, key, *args):
      return self.lpush(key, *[JSON_Encode(arg).encode('utf-8') for arg in args])

    def lrange_bool(self, key, start, stop):
      return [bool(int(s)) for s in self.lrange(key, start, stop)]

    def lrange_int(self, key, start, stop):
      return [int(s) for s in self.lrange(key, start, stop)]

    def lrange_str(self, key, start, stop):
      return [s.decode('utf-8') for s in self.lrange(key, start, stop)]

    def lrange_json(self, key, start, stop):
      return [JSON_Decode(s.decode('utf-8')) for s in self.lrange(key, start, stop)]

    def lset_bool(self, key, index, value):
      return self.lset(key, index, str(int(bool(value))))

    def lset_int(self, key, index, value):
      return self.lset(key, index, str(int(value)))

    def lset_str(self, key, index, value):
      return self.lset(key, index, str(value).encode('utf-8'))

    def lset_json(self, key, index, value):
      return self.lset(key, index, JSON_Encode(value).encode('utf-8'))

    def rpop_bool(self, key):
      value = self.rpop(key)
      return None if value is None else bool(int(value))

    def rpop_int(self, key):
      value = self.rpop(key)
      return None if value is None else int(value)

    def rpop_str(self, key):
      value = self.rpop(key)
      return None if value is None else value.decode('utf-8')

    def rpop_json(self, key):
      value = self.rpop(key)
      return None if value is None else JSON_Decode(value.decode('utf-8'))

    def rpush_bool(self, key, *args):
      return self.rpush(key, *[int(bool(arg)) for arg in args])

    def rpush_int(self, key, *args):
      return self.rpush(key, *[int(arg) for arg in args])

    def rpush_str(self, key, *args):
      return self.rpush(key, *[str(arg).encode('utf-8') for arg in args])

    def rpush_json(self, key, *args):
      return self.rpush(key, *[JSON_Encode(arg).encode('utf-8') for arg in args])


  def OpenRedis(*, Host, Database, Port=6379):
    return Redis(host=Host, db=Database, port=Port)

pass#if redis

########################################################################################################################


# html special characters.  Any false values will be converted to an empty string.
def HS(s):
  if type(s) is HTML:
    return str(s)
  else:
    return escape('' if s is None else str(s))


# quote attributes.  Any false values will be converted to an empty string.
def QA(s):
  return quoteattr('' if s is None else str(s))


# encode parts of a URL
def UE(s):
  return quote_plus('' if s is None else str(s))


# Will return a string of the following joined together.
# a. an iterator of strings, OR
# b. any iterator and a lambda to process it into strings
def JN(iterator, func=None):
  if func:
    iterator = (func(v) for v in iterator)
  return "".join(iterator)


def FK(mapping, keys, *, AllowNone=False):
  '''
  FilterKeys: return a new aadict only containing all the specified keys.
  Keys can be specified using any iterable of strings, or a comma-delimited string
  '''
  if mapping is None:
    if AllowNone:
      return None
    else:
      raise ValueError('1st paramter, `mapping`, must not be None')

  if isinstance(keys, str):
    keys = set(k.strip() for k in keys.split(','))

  return aadict((k,mapping[k]) for k in keys)
  

def FKS(mapping, keys, *, AllowNone=False, MissingValue=None):
  '''
  FilterKeys: return a new aadict only containing all the specified keys.
  Keys can be specified using any iterable of strings, or a comma-delimited string

  WILL FILL IN None by default for missing keys
  '''
  if mapping is None:
    if AllowNone:
      return None
    else:
      raise ValueError('1st paramter, `mapping`, must not be None')

  if isinstance(keys, str):
    keys = set(k.strip() for k in keys.split(','))

  return aadict((k,mapping.get(k, MissingValue)) for k in keys)
  


def FA(theobject, attrs, *, AllowNone=False):
  '''
  FilterKeys: return a new aadict only containing all the specified attributes.
  Keys can be specified using any iterable of strings, or a comma-delimited string
  '''
  if theobject is None:
    if AllowNone:
      return None
    else:
      raise ValueError('1st paramter, `theobject`, must not be None')

  if isinstance(attrs, str):
    attrs = set(k.strip() for k in attrs.split(','))

  return aadict((k,getattr(theobject, k)) for k in attrs)


def FAS(theobject, attrs, *, AllowNone=False, MissingValue=None):
  '''
  FilterKeys: return a new aadict only containing all the specified attributes.
  Keys can be specified using any iterable of strings, or a comma-delimited string
  
  WILL fill in None for missing values by default
  '''
  if theobject is None:
    if AllowNone:
      return None
    else:
      raise ValueError('1st paramter, `theobject`, must not be None')

  if isinstance(attrs, str):
    attrs = set(k.strip() for k in attrs.split(','))

  return aadict((k, (getattr(theobject, k) if hasattr(theobject, k) else MissingValue)) for k in attrs)

#######################################################################################################################
def RegisterBuiltins():
  import builtins
  builtins.aadict = aadict
  builtins.ARGS = ARGS
  builtins.AS3 = AS3
  builtins.AuthorizationError = AuthorizationError
  builtins.BOOLN = BOOLN
  builtins.COAL = COAL
  builtins.DataError = DataError
  builtins.DataNotFoundError = DataNotFoundError
  builtins.DataConflictError = DataConflictError
  builtins.DECIMALN = DECIMALN
  builtins.DT = DT
  builtins.FA = FA
  builtins.FAS = FAS
  builtins.FK = FK
  builtins.FKS = FKS
  builtins.FLOATN = FLOATN
  builtins.HS = HS
  builtins.HTML = HTML
  builtins.INTN = INTN
  builtins.JN = JN
  builtins.JSONDecoder = JSONDecoder
  builtins.JSONEncoder = JSONEncoder
  builtins.ML = ML
  builtins.NULL = NULL
  builtins.obdict = obdict
  builtins.QA = QA
  builtins.SEC = SEC
  builtins.SL = SL
  builtins.SQL = SQL
  builtins.STRN = STRN
  builtins.UE = UE
  builtins.Undefined = Undefined

  if rich:
    builtins.CONSOLE = CONSOLE
  
  if yaml:
    builtins.YAML_Encode = YAML_Encode
    builtins.YAML_Decode = YAML_Decode
 
  if postgresql:
    builtins.Connection = Connection
    builtins.NotOneFound = NotOneFound
    builtins.OpenPostgres = OpenPostgres
    builtins.PreparedStatement = PreparedStatement
    builtins.TransactionInProgressError = TransactionInProgressError
 
  if redis:
    builtins.OpenRedis = OpenRedis
    builtins.Redis = Redis
