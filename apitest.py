#! /user/bin/env python
#encoding:utf-8

from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return 'hello,world'

@app.route('/p',methods=['POST'])
def p_test():
    data = request.get_data()
    print(data)
    return 'post'


if __name__ == '__main__':
    app.run(debug=True)