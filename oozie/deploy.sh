# Build egg
cd /home/btelle/scripts/next-gen-etl/
python setup.py bdist_egg
cp dist/wb_next_gen_etl-1.0-py2.7.egg ../oozie/dist/
