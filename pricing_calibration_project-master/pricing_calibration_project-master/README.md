pricing_calibration_project
-------

pricing calibration project Application

Install
-------
## clone the repository
    git clone https://github.com/RevnomixGit/pricing_calibration_project.git
    # checkout the correct version
    git tag  # shows the tagged versions
    git checkout latest-tag-found-above
    
Create a virtualenv in the application directory and activate it::

    python -m venv venv
    venv\Scripts\activate.bat
	
    

Install Dependencies in pipenv Environment::

    pip install pipenv
    pip3 install pipenv
    pipenv install

Install Dependencies in Virtual Environment::

    pip install -r requirements.txt
    
 Help
 
     First arg is command : comp or cont or serv, or repo or enty
     Second arg is component name
     Third arg is package name

 Commands :
 
 
 
 RUN EXAMPLE :
 ------------
	RUN
 ---
 
    On Virtual Environment::
    set APP_ENV=Production
	export APP_ENV=Production
	
    set FLASK_APP=run.py
    flask run
    flask run --host=0.0.0.0
    
    
    Open http://127.0.0.1:5000 in a browser.
	