# BundesPosts

## Sentiment Analysis 

### Comparison of sentiment models

We tested three sentiment models on the publicly available “German politicians twitter Sentiment”-dataset (https://huggingface.co/datasets/Alienmaster/german_politicians_twitter_sentiment). For this evaluation we used the test split with 357 examples and acquired the following models via the Hugging Face platform: Multilingual Sentiment Classification Model, German Sentiment Bert, XLM RoBERTa German Sentiment. We employed the different models and their tokenizer to classify the tweets of the test split and then compared the results to the true labels. The code for this process as well as the overall accuracy-values and the classification reports for all three models can be found in "Sentiment_models_comparison_twitter.ipynb".

### Finetuning GBERT-base

The jupyter notebook "Finetuning_gbert1_twitter_dataset" contains the pipeline for fine-tuning the German language model GBERT-base by deepset to sentiment classification (three classes) with the “German politicians twitter Sentiment”-dataset. This fine-tuned model is referred to as GBERT1. 

The jupyter notebook "Finetuning_gbert2_twitter&germeval17" containts the pipeline for fine-tuning GBERT-base to sentiment classification with the “German politicians twitter Sentiment”-dataset and the publicly available “Germeval Task 2017”-dataset (https://huggingface.co/datasets/akash418/germeval_2017). This fine-tuned model is referred to as GBERT2. 

To ensure compatibility with our model, we mapped the sentiment labels to numerical values. Additionally, unnecessary columns were removed to retain only the tweet text and corresponding labels. For the training we used the train-split of the datasets and the test-split for evaluation. Each post was tokenized with truncation and padding to a maximum sequence length of 512 for the twitter-dataset and 128 for the combined dataset. Afterwards the tokenized datasets were converted into PyTorch tensors so that the model can process them. Each model was trained for 4 epochs and with a learning rate of 2e-5. The batch size of GBERT1 was 8 and of GBERT2 16. Moreover, the trainer was configured with an evaluation metric function that computes accuracy, precision, recall, and F1-score. 

### Evaluation of fine-tuned models 
