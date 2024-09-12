# python-template

- Run `create_from_template.sh` from the python-template/ root dir
```
> bash create_from_template.sh . ../python-template
```

- `cd ../python-template`

- Create/activate python virtualenv:
```
# Install pyenv-virtualenv if needed
> brew install pyenv-virtualenv

# Create/set env, install dependencies
> pyenv virtualenv 3.10.6 python-template-env
> pyenv local python-template-env
```


- Install requirements, make sure it's working
```
> pip install -r requirements.txt
> make tests
```


- Adding to github
Use github CLI or web UI to create the repository, `python-template` in `your-org`

```
gh repo create your-org/python-template --private
```

```
echo "# python-template" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/your-org/python-template.git
git push -u origin main
```
