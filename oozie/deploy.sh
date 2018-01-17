# Build wheel
cd /home/btelle/scripts/next-gen-etl/
python setup.py bdist_wheel
cp dist/wb_next_gen_etl-1.0-py2-none-any.whl ../oozie/dist/

cd ../oozie/dist/

# build venv
export OLDPYTHONPATH=$PYTHONPATH
export PYTHONPATH=""
virtualenv --no-site-packages etl
source etl/bin/activate
python -m pip install wb_next_gen_etl-1.0-py2-none-any.whl
deactivate
export PYTHONPATH=$OLDPYTHONPATH

# archive venv for distribution
tar cfz pylib.tar.gz etl/
rm -rf etl
