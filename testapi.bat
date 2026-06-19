@echo off
"C:\venvs\my_search_app\Scripts\python.exe" -c "import urllib.request, json; r = urllib.request.urlopen('http://127.0.0.1:8000/api/jobs?company=Flipkart&q=engineer', timeout=10); d = json.loads(r.read()); print('Flipkart:', d['count']); [print(' -', j['title'][:50], '|', j['location'][:60], '|', j['url']) for j in d['jobs'][:5]]"
echo.
"C:\venvs\my_search_app\Scripts\python.exe" -c "import urllib.request, json; r = urllib.request.urlopen('http://127.0.0.1:8000/api/jobs?company=Cars24&q=engineer', timeout=10); d = json.loads(r.read()); print('Cars24 (engineer):', d['count']); [print(' -', j['title'][:50], '|', j['location'][:60], '|', j['url']) for j in d['jobs'][:5]]"
echo.
"C:\venvs\my_search_app\Scripts\python.exe" -c "import urllib.request, json; r = urllib.request.urlopen('http://127.0.0.1:8000/api/jobs?company=Cars24&q=associate', timeout=10); d = json.loads(r.read()); print('Cars24 (associate):', d['count']); [print(' -', j['title'][:50], '|', j['location'][:60], '|', j['url']) for j in d['jobs'][:5]]"
echo.
"C:\venvs\my_search_app\Scripts\python.exe" -c "import urllib.request, json; r = urllib.request.urlopen('http://127.0.0.1:8000/api/jobs?company=Swiggy&q=manager', timeout=10); d = json.loads(r.read()); print('Swiggy (manager):', d['count']); [print(' -', j['title'][:50], '|', j['location'][:60], '|', j['url']) for j in d['jobs'][:5]]"
