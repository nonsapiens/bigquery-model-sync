<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('bigquery_syncs', function (Blueprint $table) {
            $table->id();
            $table->string('model', 191)->index();
            $table->string('sync_batch_uuid', 36)->nullable()->index();
            $table->enum('sync_type', ['create', 'batch', 'replace']);
            $table->unsignedBigInteger('records_synced')->default(0);
            $table->enum('status', ['pending', 'in_progress', 'completed', 'failed', 'cancelled'])->default('pending');
            $table->text('error_message')->nullable();
            $table->timestamp('started_at')->nullable();
            $table->timestamp('completed_at')->nullable();
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('bigquery_syncs');
    }
};
