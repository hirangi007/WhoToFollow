# WhoToFollow

Instagram and twitter functionality implemented using mapreduce program.

1. final code : https://raw.githubusercontent.com/hirangi007/WhoToFollow/master/wtf/src/Instagram_Twitter.java?token=AYIprMlXNxmBoptIiv_0nSZD7zemfI3Oks5YqfgdwA%3D%3D

2. final output of input given in pdf : https://raw.githubusercontent.com/hirangi007/WhoToFollow/master/wtf/bin/final-output/part-r-00000?token=AYIprKm2-zH86D4ct4dO0pf3bjFTMwphks5YqfjywA%3D%3D

3. FOR GENERATE.PY FILE

    input : https://raw.githubusercontent.com/hirangi007/WhoToFollow/master/wtf/bin/generate.txt?token=AYIprAi6tjHnoM1J2csq5yKhpcFwc0_Oks5YqfmtwA%3D%3D
    
    output : https://raw.githubusercontent.com/hirangi007/WhoToFollow/master/wtf/bin/generate.py-final-output/part-r-00000?token=AYIprLatEzVeq53pX8MMyBEFzyyoqsp_ks5YqfnUwA%3D%3D
    
    
STEPS TO RUN :

1. Save the program in eclipse. So class file will be generated.
2. go to the directory where .class file is saved.
3. fire command "jar cvf Instagram_Twitter.jar Instagram_Twitter*.class"
4. fire command "hadoop jar Instagram_Twitter.jar Instagram_Twitter input.txt final-output" where "input.txt" is input file name and "final-output" is output folder
    
    
