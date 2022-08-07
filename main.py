from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from samples import testing


app = Flask(__name__)


# @app.route('/')
# def index():
#     return "hello dudeee"

port = 5000

# /// = relative path, //// = absolute path
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Todo(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100))
    complete = db.Column(db.Boolean)


db.create_all()



@app.get("/test")
def test():
    testing()
    return render_template("base.html")




@app.get("/")
def home():
    # todo_list = Todo.query.all()
    todo_list = db.session.query(Todo).all()
    # return "Hello, Dude!"
    return render_template("base.html", todo_list=todo_list)

# @app.route("/add", methods=["POST"])
@app.post("/add")
def add():
    title = request.form.get("title")
    new_todo = Todo(title=title, complete=False)
    db.session.add(new_todo)
    db.session.commit()
    return redirect(url_for("home"))


@app.get("/update/<int:todo_id>")
def update(todo_id):
    # todo = Todo.query.filter_by(id=todo_id).first()
    todo = db.session.query(Todo).filter(Todo.id == todo_id).first()
    todo.complete = not todo.complete
    db.session.commit()
    return redirect(url_for("home"))


@app.get("/delete/<int:todo_id>")
def delete(todo_id):
    # todo = Todo.query.filter_by(id=todo_id).first()
    todo = db.session.query(Todo).filter(Todo.id == todo_id).first()
    db.session.delete(todo)
    db.session.commit()
    return redirect(url_for("home"))


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=port)




## For testing purposes only

# @app.get("/gettest")
# def gettest():
#     return "get success"


# @app.post("/posttest")
# def posttest():
#     title1 = request.form.get("firstName")
#     title2 = request.form
#     return request.form





## Empty flask project

# import os
# from flask import Flask

# app = Flask(__name__)

# @app.route("/")
# def hello_world():
#     name = os.environ.get("NAME", "world")
#     return "Hello {}".format(name)

# if __name__ == "__main__":
#     app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("port", 8080)))






