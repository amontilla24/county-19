# county-19
A firebase(d) app to get live updates of Covid 19 cases in the US

### TODOS
Firebase is running in test mode, need to edit authentication and permissions

Data is re-populated and sent to firebase even if repo pull remains unchanged

### Running commands for EC2

Running script in background:
sudo nohup python3 /home/ec2-user/county-19/County_19_Notebook.py>download_history.log &

Show running threads and kill thread:
ps ax
sudo kill <PID>
