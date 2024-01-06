import praw, time, datetime

TIME_TO_WAIT = 2 #time to wait between requests to server, so as not to increase load too much

def get_elapsed(current: float, previous: float) -> str:
    '''
    Return the elapsed time as a pretty string for reporting.
    Arguments:
        `current: float`: the current (later) time in seconds.
        `previous: float`: the previous (earlier) time in seconds.
    Returns:
        `str`: a string for the format f'{hours} hours, {minutes} minutes, and {seconds} seconds', 
            with hours and minutes being integers, and seconds rounded to three decimal points.
    '''
    hours = int(current-previous) // 3600
    min_begin = previous + hours * 3600
    minutes = int(current-min_begin) // 60
    sec_begin = previous + hours * 3600 + minutes * 60
    seconds = current - sec_begin
    return f'{hours} hours, {minutes} minutes, and {seconds:.3f} seconds'

def retrieve(reddit: praw.Reddit, subreddit: str, begin_time: float, end_time: float, wait_time: float=TIME_TO_WAIT) -> list[praw.models.Submission]:
    '''
    Retrieve all posts on a subreddit in a time frame.
    Arguments:
        `reddit: praw.Reddit`: the authorized Reddit instance.
        `subreddit: str`: the name of the subreddit to request posts from.
        `begin_time: str`: the first time to request submissions from.
        `end_time: str`: the last time to request submissions from.
    Returns:
        `list[praw.Submission]`: a list of all submissions made during the time period, sorted first posted to last posted.
    '''
    current, posts, i = begin_time, [], 0
    while int(current) < int(end_time):
        current_post = None
        iter_start = time.time()
        
        #actually perform query, add to results
        temp_results = reddit.subreddit(subreddit).new(params={"after": current_post.fullname} if current_post else dict())
        temp_results = list(temp_results) #this is the part that actually requests the submissions from Reddit servers
        posts += temp_results
        
        #deal with time stuff
        previous, current = current, temp_results[-1].created_utc+1
        elapsed = get_elapsed(current, previous)
        remaining = get_elapsed(end_time, current)
        
        #report
        iter_time = time.time() - iter_start
        iter_elapsed = get_elapsed(iter_time, iter_start)
        time_rate = (iter_time) / (current - previous)
        remaining_time = (end_time - current) * time_rate
        remaining_iter = remaining_time / iter_time
        remaining_elapsed = get_elapsed(remaining_time, 0)
        print(f'Retrieved {i:,},000th post, spanning {elapsed} of real time.
        print(f'\tFinished iteration in {iter_time:.2f}.')
        print(f'\t{remaining} of real time remain.')
        print(f'\tAt this rate, expecting to finish {remaining_iter:.0f} iterations in {remaining_elapsed}.')
        print(f'\tWaiting {wait_time} seconds before next iteration.')
        
        #cleanup
        i += 1
        current_post = posts[-1]
        time.sleep(wait_time) #don't want to work those servers too hard
    return [post for post in posts if post.created_utc < end_time][::-1]