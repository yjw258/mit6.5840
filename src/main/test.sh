cmp mr-wc-all mr-correct-wc
if [ $? -eq 0 ]; then
    echo "Files are the same"
else
    echo "Files are different"
fi
