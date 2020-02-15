# Sample taken from pyStrich GitHub repository
# https://github.com/mmulqueen/pyStrich

from flask import Flask
from pystrich.datamatrix import DataMatrixEncoder

app = Flask(__name__)
@app.route("/")
def hello():
    encoder = DataMatrixEncoder('This is a DataMatrix.')
    encoder.save('./datamatrix_test.png')
    print(encoder.get_ascii())
    return "Hello World!"
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int("5000"), debug=True)



