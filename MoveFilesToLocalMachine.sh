#!/bin/bash
$docker_container_name =  docker ps --format '{{.Names}}'


$cp_arguments = @("cp",                                               `
                  "$($docker_container_name):/src/resultados",  `
                  "./")

docker $cp_arguments