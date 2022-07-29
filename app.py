# vim:fileencoding=utf-8:ts=4:sw=4:sts=4:expandtab

from flask import Flask, escape, Markup

import Granite
from Granite import QA, HS

Redis = Granite.OpenRedis(Host='redis', Database=0)

app = Flask(__name__)

@app.route("/")
def hello_world():

    
    num = Redis.get_int('count') or 0

    Redis.set_int('count', num+1)
    
    UI = Layout()
    UI('''
        <h1>Hello visitor #''' + HS(num) + ''', this is an example flask app</h1>
        <hr>
        Look at <code>app.py</code> to see how it works.
        <hr>
        <img style="width: 400px; height: 400px;" src="/static/svg.svg">
        
    ''')
    
    return UI.Render()

@app.route("/hi")
def hi():
    UI = Layout()
    UI('''
        hi
    ''')
    return UI.Render()

class Layout():
    def __init__(self):
        self.Container = True
        self.Body = []

    def __call__(self, content):#self, content):
        self.Body.append(str(content))

    def Render(self):
        return ('''<!doctype html>
<html lang="en">
  <head>
    <!-- Required meta tags -->
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC" crossorigin="anonymous">
    <link href="/static/app.css" rel="stylesheet"  crossorigin="anonymous">

    <title>Hello, world!</title>
  </head>
  <body>
    ''' + ('''
    <div class="container">
        <nav>
            <a href="/">home</a>
            <a href="/hi">hi</a>
        </nav>
        ''' + ''.join(self.Body) + '''
    </div>
    ''' if self.Container else '''
        ''' + ''.join(self.Body) + '''
    ''') + '''

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-MrcW6ZMFYlzcLA8Nl+NtUVF0sA7MsXsP1UyJoMp4YLEuNSfAP+JcXn/tWtIaxVXM" crossorigin="anonymous"></script>
  </body>
</html>
''')


