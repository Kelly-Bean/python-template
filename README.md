# python-template

- Run `create_from_template.sh` from the python-template/ root dir
```
> bash create_from_template.sh . ../your-repo-name
```

- `cd ../your-repo-name`

- Create/activate python virtualenv:
conda:
```
> conda create python=3.11 -n python-template-env
> conda activate python-template-env
```

- Install requirements, make sure it's working
```
> pip install -r requirements.txt
> make tests
```


- Git
Use github CLI or web UI to create the repository, `your-repo-name` in `your-org`

```
gh repo create your-org/your-repo-name --private
```

```
echo "# your-repo-name" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/your-org/your-repo-name.git
git push -u origin main
```
