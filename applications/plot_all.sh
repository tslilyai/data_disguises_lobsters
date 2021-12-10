#!/bin/bash

python3 plot_lobsters_concurrent.py
python3 plot_websubmit_concurrent.py
python3 plot_stats.py
python3 plot_composition_stats.py
python3 plot_enctest_results.py
cp *.pdf ../papers/osdi22/figs/
