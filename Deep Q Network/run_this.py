from market_env import market_env
from DeepQNetwork import DeepQNetwork
import numpy as np
import tensorflow as tf
import time

def run():
    step = 0
    tf.reset_default_graph()
    for episode in range(0,30000):
        
        
        while True:
            #start_time = time.time()
            a = 0
            
            env.get_data(a)
            
            observation = np.asarray(env.observation)            
            action = RL.choose_action(observation,env.action_space)
            observation_, reward, done = env.step(action)

            RL.store_transition(observation, action, reward, observation_)
            #print(time.time()-start_time)
            
            a+=1
            if (step > 200) and (step % 5 == 0):
                RL.learn()

            
            observation = observation_
            step +=1
            
            if done:
                break
            
        episode +=1
    print('game over')
    env.reset()

if __name__ == "__main__":
    env = market_env()
    RL = DeepQNetwork(env.n_actions, env.n_features,
                      learning_rate=0.01,
                      reward_decay=0.9,
                      e_greedy=0.9,
                      replace_target_iter=200,
                      memory_size=2000,
                      output_graph=True
                      )
    run()
    RL.plot_cost()