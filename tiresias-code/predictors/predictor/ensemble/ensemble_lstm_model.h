#pragma once

#include <torch/torch.h>
#include <vector>

struct ensemble_lstm_model : torch::nn::Module {
    torch::nn::LSTM   lstm;
    torch::nn::Linear fc;

    long input_size;
    long hidden_size;
    long output_size;

    std::tuple<torch::Tensor, torch::Tensor> hidden_cell;

    ensemble_lstm_model( long input_size, long hidden_size, long num_layers,
                         long output_size );
    torch::Tensor forward( torch::Tensor input );
};
