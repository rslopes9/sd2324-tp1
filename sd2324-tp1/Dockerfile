FROM smduarte/sd2324testerbase

# working directory inside docker image
WORKDIR /home/sd

ADD hibernate.cfg.xml .
ADD shorts.props .

# copy the jar created by assembly to the docker image
COPY target/*jar-with-dependencies.jar sd2324.jar
