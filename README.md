Maker Tools (Python)
====================

```
pip install -r requirements.txt
cp config.ini.example config.ini
```

Update config.ini with your authorization key and wallet id

Run

```
./qa.py --chain 1 --maker mm5 --env production
```


For more details on the QA script, please see the documentation in the typescript [maker-tools](https://github.com/hashflownetwork/maker-tools) repo. Note that
the typescript tool takes arguments in the form: 

```
--chain=1
```

while this python script uses a different convention:

```
--chain 1
```


