mode="normal"
threads=100
if [ -n "$2" ]; then
  mode="$2"
fi

if [ -n "$3" ]; then
  threads="$3"
fi

if [ -n  "$1" ]; then
  echo "launching client for servers $1"
  go run main.go --mode="$mode" --threads="$threads" --servers="$1"

else
  echo "put servers ip: example 127.0.0.1:8080;127.0.0.1:8081;127.0.0.1:8082"
fi