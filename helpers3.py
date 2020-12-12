#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import math


def add_col_with_condition(df, column_name, condition):
    """
        Extract data from a DataFrame's column to another new column  a following condition,
        append 0 or 1 to the new column if the data follow the condition or not
        :param df: the relevant DataFrame
        :param column_name: the column of the dataframe to perform the extraction
        :param condition: the data we are looking for into the column
        :return the transformed dataframe
    """
    condition_list = list()
    for i in range(len(df[column_name])):
        if condition in df[column_name][i]:
            condition_list.append(1)
        else:
            condition_list.append(0)

    df[condition] = condition_list

    return df

def portfolio_exploration(df):
    """
        Exploration of the portfolio dataframe:
            - divide the channels column using the function add_col_with_condition
            with the conditions: 'web', 'email', 'mobile' and 'social'
            - drop any unnecessary columns
            - convert the offer ids into simplier numerical value
        :param df: the relevant DataFrame
        :return the transformed dataframe
    """

    portfolio_df = df

    # Channels column split
    # Add four new columns with the 0 or 1 as value
    portfolio_df = add_col_with_condition(portfolio_df, 'channels', 'web')
    portfolio_df = add_col_with_condition(portfolio_df, 'channels', 'email')
    portfolio_df = add_col_with_condition(portfolio_df, 'channels', 'mobile')
    portfolio_df = add_col_with_condition(portfolio_df, 'channels', 'social')

    # Convert the offer ids
    offer_list = list()
    for i in range(len(portfolio_df['offer_type'])):
        if 'informational' in portfolio_df['offer_type'][i]:
            offer_list.append(1)
        elif 'discount'in portfolio_df['offer_type'][i]:
            offer_list.append(2)
        elif 'bogo'in portfolio_df['offer_type'][i]:
            offer_list.append(3)
        else:
            offer_list.append('error')

    portfolio_df['offer_type'] = offer_list
        
    # Normalise the reference of the offer, update the index and
    # drop the column 'channels' and 'id'
    drop = ["channels" , "id"]
    portfolio_df = portfolio_df.drop(columns = drop)
    portfolio_df.set_index(pd.Index([1,2,3,4,5,6,7,8,9,10]), inplace = True)

    return portfolio_df

def profile_exploration(df):
    """
        Exploration of the profile dataframe:
            - normalise the id's references (use the index as references)
            - convert the genders into numerical values
            - replace the missing incomes by the mean of incomes
            #- normalise the age, the date and the income into normalized range
        :param df: the relevant DataFrame
        :return the transformed dataframe
    """
    profile_df = df

    # normalise the id's references
    profile_df = df.drop(columns = 'id')

    # convert the gender
    gender_list = list()
    for i in range(len(profile_df['gender'])):
        if profile_df['gender'][i] == None:
            gender_list.append(0)
        elif profile_df['gender'][i] == "O":
            gender_list.append(1)
        elif profile_df['gender'][i] == "F":
            gender_list.append(2)
        elif profile_df['gender'][i] == "M":
            gender_list.append(3)

    profile_df['gender'] = gender_list
    profile_df.isna().sum()

    # Replace the missing incomes by the mean of incomes
    profile_df['income'].fillna(value=profile_df['income'].mean(), inplace=True)

    return profile_df

def transcript_value_exploration(transcript_raw_df, offer_id_dict, profile_id_dict):
    """
        Exploration of the values in the transcript dataframe:
            - split the value column into 3 new columns: offer id, amount, reward
            - replace the offer id and the person id by their references from two
            dictionaries
            - create two new references for each lines:
                - one using the person id and the offer id
                - and another using the person id, the offer id and the time
        :param transcript_raw_df: the relevant DataFrame
        :param offer_id_dict: the dictionary containing the offer references
        :param profile_id_dict: the dictionary containing the profile references
        :return the transformed dataframe
    """
    offer_id_list = list()
    amount_list = list()
    reward_list = list()
    person_list = list()
    pers_offer_time_list = list()
    pers_offer_list = list()

    for i in range(len(transcript_raw_df['value'])):
        if 'offer id' in transcript_raw_df['value'][i]:
            offer_id_list.append(offer_id_dict[transcript_raw_df['value'][i]['offer id']])
        elif 'offer_id' in transcript_raw_df['value'][i]:
            offer_id_list.append(offer_id_dict[transcript_raw_df['value'][i]['offer_id']])
        else:
            offer_id_list.append(0)

        if 'amount' in transcript_raw_df['value'][i]:
            amount_list.append(transcript_raw_df['value'][i]['amount'])
        else:
            amount_list.append(0)

        if 'reward' in transcript_raw_df['value'][i]:
            reward_list.append(transcript_raw_df['value'][i]['reward'])
        else:
            reward_list.append(0)

        person_list.append(profile_id_dict[transcript_raw_df['person'][i]])

        pers_offer_time_list.append(str(person_list[i]) + "-" + str(offer_id_list[i]) + "-" + str(transcript_raw_df['time'][i]))
        pers_offer_list.append(str(person_list[i]) + "-" + str(offer_id_list[i]))

    transcript_value_clean_df = transcript_raw_df
    transcript_value_clean_df.insert(0, 'pers-offer-time', pers_offer_time_list )
    transcript_value_clean_df['offer id'] = offer_id_list
    transcript_value_clean_df['person'] = person_list
    transcript_value_clean_df['amount'] = amount_list
    transcript_value_clean_df['reward'] = reward_list
    transcript_value_clean_df['pers-offer'] = pers_offer_list


    col = ['value']
    transcript_value_clean_df = transcript_value_clean_df.drop(columns = col)

    return transcript_value_clean_df

def transcript_transaction_exploration(df):
    """
        Exploration of the transaction in the transcript dataframe:
            - check if a transaction is linked to an offer or not:
            => if it is linked, the event is labeled as "transaction linked" and
               its person offer id is updated with the associated offer
            - use of the time and event label to perfrom the comparaison
            - update the associated offer amount
        :param df: the relevant DataFrame
        :return the transformed dataframe
    """
    transcript_pre_cleaning_df = df

    for i in range(len(transcript_pre_cleaning_df['event']) -1):
        if transcript_pre_cleaning_df.at[i,'event'] == 'transaction':
            my_transaction_time = transcript_pre_cleaning_df.at[i,'time']
            my_transaction_amount = transcript_pre_cleaning_df.at[i,'amount']

            offer_linked_list = list()

            for j in range(i+1, len(transcript_pre_cleaning_df['event'])):
                if 'offer completed' in transcript_pre_cleaning_df.at[j,'event']:
                    if transcript_pre_cleaning_df.at[j,'time'] == my_transaction_time:

                        pers_offer_index = transcript_pre_cleaning_df.at[j,'pers-offer']

                        transcript_pre_cleaning_df.at[i, 'event'] = 'transaction linked'
                        transcript_pre_cleaning_df.at[i, 'pers-offer'] = pers_offer_index

                        offer_linked_list.append(j)
                    else:
                        break
                else:
                    break

            numb_linked_offer = len(offer_linked_list)

            # Update the associated offer amount, if a transaction is linked to several offer,
            # the amount is equally shared between the offers
            if numb_linked_offer != 0:
                for offer in offer_linked_list:
                    transcript_pre_cleaning_df.at[offer, 'amount'] = my_transaction_amount / numb_linked_offer

    return transcript_pre_cleaning_df


def transcript_final_exploration(df):
    """
        Create a new dataframe based on the transcript dataframe:
            - the event column is split into: received, viewd and completed
            => the goal is to have one line per person/offers
            - add a new column "completed before viewed" to check if an offer has been
            completed before being viewed
            - we use 0 in the column offer as reference for the transaction without
            links to an offer
            - for each couple person/offer or person/transaction, we will count how many time
            the offer has been received, viewed, completed and completed before being viewed,
            the default value for the couple transaction will be 1
            - if an offer has been completed several times, the sum of the amounts will
            be added, same process for the rewards
        :param df: the relevant DataFrame
        :return the transformed dataframe
    """
    transcript_final_cleaning_df = df

    # initialisation of the list and dict
    final_pers_offer_dict = dict()
    final_pers_offer_time_dict = dict()
    final_received_list = list()
    final_viewed_list = list()
    final_completed_list = list()
    final_completed_before_viewed_list = list()
    final_amount_list = list()
    final_reward_list = list()
    final_pers_list = list()
    final_offer_list = list()

    list_index = 0

    for i in range(len(transcript_final_cleaning_df['pers-offer-time'])):
        my_pers_offer = transcript_final_cleaning_df.at[i, 'pers-offer']
        my_pers_offer_time = transcript_final_cleaning_df.at[i, 'pers-offer-time']
        my_status = transcript_final_cleaning_df.at[i, 'event']
        my_amount = transcript_final_cleaning_df.at[i, 'amount']


        if my_status == "transaction":
            # add a new line for the transaction without offer
            final_pers_offer_time_dict[my_pers_offer_time] = list_index
            transaction_only_text = my_pers_offer + "-" + str(i)
            final_pers_offer_dict[transaction_only_text] = list_index
            final_received_list.append(1)
            final_viewed_list.append(1)
            final_completed_list.append(1)
            final_completed_before_viewed_list.append(0)
            final_amount_list.append(transcript_final_cleaning_df.at[i, 'amount'])
            final_reward_list.append(transcript_final_cleaning_df.at[i, 'reward'])
            final_pers_list.append(transcript_final_cleaning_df.at[i, 'person'])
            final_offer_list.append(transcript_final_cleaning_df.at[i, 'offer id'])

            list_index += 1

        else:
            # Check if pers-offer is already present in the offer list
            if my_pers_offer in final_pers_offer_dict:
                offer_index_in_ref =  final_pers_offer_dict[my_pers_offer]

                #check if it has already been received
                if my_status == "offer received":
                    final_received_list[offer_index_in_ref] += 1

                elif my_status == "offer viewed":
                    final_viewed_list[offer_index_in_ref] += 1

                elif my_status == "offer completed":
                    final_completed_list[offer_index_in_ref] += 1
                    final_reward_list[offer_index_in_ref] += transcript_final_cleaning_df.at[i, 'reward']
                    final_amount_list[offer_index_in_ref] += my_amount

                    if final_completed_list[offer_index_in_ref] > final_viewed_list[offer_index_in_ref]:
                        final_completed_before_viewed_list[offer_index_in_ref] += 1


            else:
                #create a new line
                final_pers_offer_time_dict[my_pers_offer_time] = list_index
                final_pers_offer_dict[my_pers_offer] = list_index
                final_received_list.append(1)
                final_viewed_list.append(0)
                final_completed_list.append(0)
                final_completed_before_viewed_list.append(0)
                final_amount_list.append(transcript_final_cleaning_df.at[i, 'amount'])
                final_reward_list.append(transcript_final_cleaning_df.at[i, 'reward'])
                final_pers_list.append(transcript_final_cleaning_df.at[i, 'person'])
                final_offer_list.append(transcript_final_cleaning_df.at[i, 'offer id'])

                list_index += 1

    # Convert the dict in list to be used in the dataframe
    final_pers_offer_time_list = final_pers_offer_time_dict.keys()
    final_pers_offer_list = final_pers_offer_dict.keys()

    col_name = ['pers-offer-time', 'received', 'viewed',
                'completed', 'completed before viewed', 'amount', 'reward', 'person',
                'offer','pers-offer']

    transcript_final_cleaning_df = pd.DataFrame(list(zip(final_pers_offer_time_list,final_received_list,
                                                final_viewed_list, final_completed_list,
                                                final_completed_before_viewed_list, final_amount_list,
                                                final_reward_list, final_pers_list, final_offer_list,
                                                final_pers_offer_list)),columns = col_name)

    return transcript_final_cleaning_df

def merge_with_portfolio(merged_data_df, portfolio_clean_df):
    
    difficulty_list = list()
    duration_list = list()
    offer_type_list = list()
    reward_list = list()
    web_list = list()
    email_list = list()
    mobile_list = list()
    social_list = list()

    for offer in merged_data_df['offer']:
        if offer != 0:  
            difficulty_list.append(portfolio_clean_df.at[offer, 'difficulty'])
            duration_list.append(portfolio_clean_df.at[offer, 'duration'])
            offer_type_list.append(portfolio_clean_df.at[offer, 'offer_type'])
            reward_list.append(portfolio_clean_df.at[offer, 'reward'])
            web_list.append(portfolio_clean_df.at[offer, 'web'])
            email_list.append(portfolio_clean_df.at[offer, 'email'])
            mobile_list.append(portfolio_clean_df.at[offer, 'mobile'])
            social_list.append(portfolio_clean_df.at[offer, 'social'])

        else:
            difficulty_list.append(0)
            duration_list.append(0)
            offer_type_list.append(0)
            reward_list.append(0)
            web_list.append(0)
            email_list.append(0)
            mobile_list.append(0)
            social_list.append(0)


    merged_data_df['difficulty'] = difficulty_list
    merged_data_df['duration'] = duration_list
    merged_data_df['offer_type'] = offer_type_list
    merged_data_df['reward'] = reward_list
    merged_data_df['web'] = web_list
    merged_data_df['email'] = email_list
    merged_data_df['mobile'] = mobile_list
    merged_data_df['social'] = social_list
    
    return merged_data_df

def merge_with_profile(merged_data_df, profile_clean_df):
    age_list = list()
    became_member_on_list = list()
    gender_list = list()
    income_list = list()

    for person in merged_data_df['person']:
        age_list.append(profile_clean_df.at[person, 'age'])
        became_member_on_list.append(profile_clean_df.at[person, 'became_member_on'])
        gender_list.append(profile_clean_df.at[person, 'gender'])
        income_list.append(profile_clean_df.at[person, 'income'])

    merged_data_df['age'] = age_list
    merged_data_df['became member on'] = became_member_on_list
    merged_data_df['gender'] = gender_list
    merged_data_df['income'] = income_list
    
    return merged_data_df