# load python module
# module load python

# switch to your prefered conda env
# source activate yourenv

# install callback module
pip install git+https://github.com/kbase/JobRunner@callback_module

# start podman service
podman system service -t 0 &
